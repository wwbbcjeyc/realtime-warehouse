package com.zjtd.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.zjtd.app.func.DimSinkFunction;
import com.zjtd.app.func.TableProcessFunction;
import com.zjtd.app.ods.MyDeserializerFunc;
import com.zjtd.bean.TableProcess;
import com.zjtd.utils.MyKafkaUtil;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BaseDBApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //并行度设置为Kafka的分区数

        //1.1 开启CK
        //        env.enableCheckpointing(5000L);
        //        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //        //正常Cancel任务时,保留最后一次CK
        //        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //        //重启策略
        //        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        //        //状态后端
        //        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink-201109/ck"));
        //        //设置访问HDFS的用户名
        //        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.读取Kafka ods_base_db 主题的数据
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app_group";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4.过滤空值数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(jsonObj -> {
            String data = jsonObj.getString("data");
            return data != null && data.length() > 0;
        });

        //TODO 5.使用FlinkCDC读取配置表并创建广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-realtime")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializerFunc())
                .build();
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("bc-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);

        //TODO 6.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        //TODO 7.处理广播流数据,发送至主流,主流根据广播流的数据进行处理自身数据(分流)
        OutputTag<JSONObject> hbaseOutputTag = new OutputTag<JSONObject>("hbase") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaFactDS = connectedStream.process(new TableProcessFunction(hbaseOutputTag, mapStateDescriptor));

        //测试打印
        DataStream<JSONObject> hbaseDimDS = kafkaFactDS.getSideOutput(hbaseOutputTag);
        kafkaFactDS.print("Kafka>>>>>>>>>>>");
        hbaseDimDS.print("HBase>>>>>>>>>>>");

        //TODO 8.将HBase流写入HBase
        hbaseDimDS.addSink(new DimSinkFunction());

        //TODO 9.将Kafka流写入Kafka
        kafkaFactDS.addSink(MyKafkaUtil.getFlinkKafkaProducerBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sinkTable"),
                        element.getString("data").getBytes());
            }
        }));

        //TODO 10.启动
        env.execute();

    }
}
