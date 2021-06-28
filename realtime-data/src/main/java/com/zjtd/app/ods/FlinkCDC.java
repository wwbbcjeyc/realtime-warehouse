package com.zjtd.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.zjtd.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        //2.使用CDC作为Source读取MySQL变化数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource
                .<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-flink")
//                .tableList("gmall-flink-201109.base_trademark")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyDeserializerFunc())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //3.将数据写入Kafka
        String topic = "ods_base_db";
        dataStreamSource.addSink(MyKafkaUtil.getFlinkKafkaProducer(topic));

        //4.启动
        env.execute();
    }
}
