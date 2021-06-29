package com.zjtd.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zjtd.bean.VisitorStats;
import com.zjtd.utils.ClickHouseUtil;
import com.zjtd.utils.DateTimeUtil;
import com.zjtd.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

//数据：Web/App -> Nginx -> SpringBoot -> Kafka(ods) -> Flink -> Kafka(dwd) -> Flink -> Kafka(dwm) -> Flink -> Kafka(dws)

//进程: Mock    -> Nginx -> Logger     -> Kafka(ZK)  -> BaseLogApp -> Kafka -> uvApp/ujApp -> Kafka -> VisitorStatsApp -> Kafka

/**
 * 访客主题宽表
 */
public class VisitorStatsApp {

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

        //TODO 2.读取Kafka主题的数据
        String groupId = "visitor_stats_app_1109";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(userJumpDetailSourceTopic, groupId));

        //TODO 3.将各个流中的数据转换为统一格式
        //3.1 处理pageDS -> pv,进入页面数,连续访问时长
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvSvDt = pageDS.map(line -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(line);
            //取出公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            //取出上一跳页面信息
            String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");
            Long sv = 0L;
            if (lastPage == null || lastPage.length() <= 0) {
                sv = 1L;
            }

            //封装统一格式对象并返回
            return new VisitorStats("",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    sv,
                    0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts"));
        });
        //3.2 处理uvDS -> UV
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUv = uvDS.map(line -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(line);
            //取出公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            //封装JavaBean并返回
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts"));
        });
        //3.3 处理ujDS -> UserJump
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUj = ujDS.map(line -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(line);
            //取出公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            //封装JavaBean并返回
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        //TODO 4.Union各个流并提取时间戳生成WaterMark
        DataStream<VisitorStats> unionDS = visitorStatsWithPvSvDt
                .union(visitorStatsWithUv, visitorStatsWithUj)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));


        //TODO 5.按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = unionDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return new Tuple4<>(visitorStats.getVc(),
                        visitorStats.getCh(),
                        visitorStats.getAr(),
                        visitorStats.getIs_new());
            }
        });

        //TODO 6.聚合
        SingleOutputStreamOperator<VisitorStats> result = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {

                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                        value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                        value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                        value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());

                        return value1;
                    }
                }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {

                        //提取数据
                        VisitorStats visitorStats = input.iterator().next();

                        //补充窗口时间字段
                        long start = window.getStart();
                        long end = window.getEnd();

                        String stt = DateTimeUtil.toYMDhms(new Date(start));
                        String edt = DateTimeUtil.toYMDhms(new Date(end));

                        visitorStats.setStt(stt);
                        visitorStats.setEdt(edt);

                        out.collect(visitorStats);
                    }
                });

        //TODO 7.将数据写出到ClickHouse
        result.print(">>>>>>>>>>");
        result.addSink(ClickHouseUtil.getSinkFunc("insert into visitor_stats_201109 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8.启动
        env.execute();
    }

}

