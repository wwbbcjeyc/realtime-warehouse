package com.zjtd.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zjtd.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 用户跳出
 */
public class UserJumpDetailApp {

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
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_app_1109";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));
        //DataStreamSource<String> kafkaDS = env.socketTextStream("hadoop102", 9999);

        //TODO 3.转换为JSON对象并提取数据中的时间戳生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        WatermarkStrategy<JSONObject> watermarkStrategy = WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                });
        SingleOutputStreamOperator<JSONObject> jsonObjWithWM = jsonObjDS.assignTimestampsAndWatermarks(watermarkStrategy);

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWM.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        //提取数据中上一跳页面
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() <= 0;
                    }
                }).times(2)
                .consecutive()
                .within(Time.seconds(10));

        //TODO 6.将模式序列作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取事件(包含超时事件)
        OutputTag<String> outputTag = new OutputTag<String>("TimeOut") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, String>() {
            //处理超时事件
            @Override
            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                JSONObject start = map.get("start").get(0);
                return start.toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            //处理匹配上的数据
            @Override
            public String select(Map<String, List<JSONObject>> map) throws Exception {
                JSONObject start = map.get("start").get(0);
                return start.toJSONString();
            }
        });

        //TODO 8.合并主流(匹配上的事件)和侧输出流(超时事件)
        DataStream<String> sideOutput = selectDS.getSideOutput(outputTag);
        DataStream<String> result = selectDS.union(sideOutput);

        //TODO 9.写入Kafka
        result.print();
        result.addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));

        //TODO 10.启动
        env.execute();

    }
}
