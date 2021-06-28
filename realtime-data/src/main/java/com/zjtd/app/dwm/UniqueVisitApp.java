package com.zjtd.app.dwm;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.zjtd.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * 访客uv
 */
public class UniqueVisitApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
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

        //TODO 2.读取Kafka dwd_page_log 主题数据
        String groupId = "unique_visit_app_1109";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        FlinkKafkaConsumer<String> flinkKafkaConsumer = MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(flinkKafkaConsumer);

        //TODO 3.将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.使用状态编程的方式过滤数据
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            //定义状态
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);
                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stringValueStateDescriptor.enableTimeToLive(ttlConfig);
                lastVisitState = getRuntimeContext().getState(stringValueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                //取出状态数据
                String lastVisit = lastVisitState.value();

                //取出当前数据中的时间
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                String curDate = sdf.format(value.getLong("ts"));

                if (lastVisit == null || !lastVisit.equals(curDate)) {
                    //今天的新访问用户
                    lastVisitState.update(curDate);
                    return true;
                }

                return false;
            }
        });

        //TODO 6.打印同时写入Kafka
        uvDS.print();
        uvDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));

        //TODO 7.启动
        env.execute();

    }

}
