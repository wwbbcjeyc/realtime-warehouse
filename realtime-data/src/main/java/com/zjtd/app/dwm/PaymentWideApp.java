package com.zjtd.app.dwm;

import com.alibaba.fastjson.JSON;
import com.zjtd.bean.OrderWide;
import com.zjtd.bean.PaymentInfo;
import com.zjtd.bean.PaymentWide;
import com.zjtd.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

//数据  Web/App -> Nginx -> SpringBoot -> Mysql(Binlog) -> FlinkApp -> Kafka(ods) -> Flink -> Kafka(dwd)/HBase(DIM) -> Flink -> Kafka(dwm) -> Flink ->  Kafka(dwm)

//进程     Mock -> Mysql(Binlog) -> FlinkCDC -> Kafka(ods_base_db) -> BaseDBApp -> Kafka(dwd_*) -> OrderWideApp(Redis) -> Kafka(dwm_order_wide) -> PaymentWideApp -> Kafka(dwm_payment_wide)

/**
 * 支付宽表
 */
public class PaymentWideApp {

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

        //TODO 2.读取Kafka主题数据 dwm_order_wide dwd_payment_info
        String groupId = "payment_wide_group_1109";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        SingleOutputStreamOperator<String> orderWideStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentInfoStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(paymentInfoSourceTopic, groupId));


        //TODO 3.将数据转换为JavaBean,并提取数据中的时间戳生成WaterMark
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        String create_time = element.getCreate_time();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(create_time).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return recordTimestamp;
                        }
                    }
                }));
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoStrDS.map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        String create_time = element.getCreate_time();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(create_time).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return recordTimestamp;
                        }
                    }
                }));

        //TODO 4.双流JOIN
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //TODO 5.将数据写入Kafka dwm_payment_wide 主题
        paymentWideDS.print();
        paymentWideDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer(paymentWideSinkTopic));

        //TODO 6.启动任务
        env.execute();

    }

}

