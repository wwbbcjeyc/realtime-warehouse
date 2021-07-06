package com.zjtd.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zjtd.app.func.DimAsyncFunction;
import com.zjtd.bean.OrderWide;
import com.zjtd.bean.PaymentWide;
import com.zjtd.bean.ProductStats;
import com.zjtd.common.GmallConstant;
import com.zjtd.utils.ClickHouseUtil;
import com.zjtd.utils.DateTimeUtil;
import com.zjtd.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Desc: 形成以商品为准的统计  曝光 点击  购物车  下单 支付  退单  评论数 宽表
 */
public class ProductStatsApp {

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

        //TODO 2.读取Kafka主题的数据创建流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        DataStreamSource<String> pageViewDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> cartDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> payWideDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refundDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(commentInfoSourceTopic, groupId));

        //TODO 3.将数据转换为统一的JavaBean

        //3.1 点击和曝光
        SingleOutputStreamOperator<ProductStats> productStatsWithPageDS = pageViewDS.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {

                //将数据转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                //获取数据时间
                Long ts = jsonObject.getLong("ts");

                //获取页面信息
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");

                String itemType = page.getString("item_type");

                if ("good_detail".equals(pageId) && "sku_id".equals(itemType)) {

                    //取出被点击的商品ID
                    Long item = page.getLong("item");

                    out.collect(ProductStats.builder()
                            .sku_id(item)
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                //取出曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");

                //判断是否为曝光数据
                if (displays != null && displays.size() > 0) {

                    for (int i = 0; i < displays.size(); i++) {

                        JSONObject displayJsonObj = displays.getJSONObject(i);

                        //取出当前曝光的数据类型
                        String item_type = displayJsonObj.getString("item_type");

                        //商品曝光数据
                        if ("sku_id".equals(item_type)) {
                            out.collect(ProductStats.builder()
                                    .sku_id(displayJsonObj.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        //3.2 收藏
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(line -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(line);

            //封装对象并返回
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.3 加购数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(line -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(line);

            //封装对象并返回
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(jsonObject.getLong("sku_num"))
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.4 订单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDS.map(line -> {

            //将数据转换为OrderWide
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

            //创建集合用于存放订单Id,考虑去重
            HashSet<Long> hashSet = new HashSet<>();
            hashSet.add(orderWide.getOrder_id());

            //封装对象并返回
            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getTotal_amount())
                    .orderIdSet(hashSet)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        //3.5 支付数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPayDS = payWideDS.map(line -> {

            //将数据转换为PaymentWide
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);

            //创建集合用于存放订单ID
            HashSet<Long> hashSet = new HashSet<>();
            hashSet.add(paymentWide.getOrder_id());

            //封装对象并返回
            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getTotal_amount())
                    .paidOrderIdSet(hashSet)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        //3.6 退单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(line -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(line);

            //创建集合用于存放订单ID
            HashSet<Long> hashSet = new HashSet<>();
            hashSet.add(jsonObject.getLong("order_id"));

            //封装对象并返回
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(hashSet)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.7 评价数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(line -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(line);

            //获取评价的类型数据
            String appraise = jsonObject.getString("appraise");
            Long goodCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                goodCt = 1L;
            }

            //封装对象并返回
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //TODO 4.将各个流Union
        DataStream<ProductStats> unionDS = productStatsWithPageDS.union(
                productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPayDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS);

        //TODO 5.提取时间戳生成Watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 6.分组、开窗、聚合
        KeyedStream<ProductStats, Long> keyedStream = productStatsWithWmDS.keyBy(ProductStats::getSku_id);
        SingleOutputStreamOperator<ProductStats> reduceDS = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {

                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());

                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());

                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());

                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {

                        //取出数据
                        ProductStats productStats = input.iterator().next();

                        //处理开窗开始和结束时间
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                        //处理订单数量
                        productStats.setOrder_ct(productStats.getOrderIdSet().size() + 0L);
                        productStats.setPaid_order_ct(productStats.getPaidOrderIdSet().size() + 0L);
                        productStats.setRefund_order_ct(productStats.getRefundOrderIdSet().size() + 0L);

                        //写出数据
                        out.collect(productStats);
                    }
                });

        //TODO 7.关联维度数据
        //7.1 关联SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getId(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) {

                        //取出维度信息
                        BigDecimal price = dimInfo.getBigDecimal("PRICE");
                        String sku_name = dimInfo.getString("SKU_NAME");
                        Long spu_id = dimInfo.getLong("SPU_ID");
                        Long tm_id = dimInfo.getLong("TM_ID");
                        Long category3_id = dimInfo.getLong("CATEGORY3_ID");

                        //将信息写入productStats对象
                        productStats.setSku_price(price);
                        productStats.setSku_name(sku_name);
                        productStats.setSpu_id(spu_id);
                        productStats.setTm_id(tm_id);
                        productStats.setCategory3_id(category3_id);
                    }

                },
                60,
                TimeUnit.SECONDS);

        //7.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getId(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        },
                        60,
                        TimeUnit.SECONDS);

        //7.3 关联TradeMark维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getId(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //7.4 关联Category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithTmDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getId(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //TODO 8.写入ClickHouse
        productStatsWithCategory3DS.print();
        productStatsWithCategory3DS
                .addSink(ClickHouseUtil.getSinkFunc("insert into product_stats_201109 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.启动
        env.execute();
    }

}
