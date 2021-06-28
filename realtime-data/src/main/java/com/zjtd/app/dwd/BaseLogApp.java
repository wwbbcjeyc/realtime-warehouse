package com.zjtd.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zjtd.utils.MyKafkaUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
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
        //        System.setProperty("HADOOP_USER_NAME", "xxx");

        //TODO 2.读取Kafka ods_base_log 主题的数据
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app_group";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSONObject
        OutputTag<String> dirty = new OutputTag<String>("DirtyData") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirty, value);
                }
            }
        });

        //TODO 4.按照设备ID分组、使用状态编程做新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlag = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    //定义状态
                    private ValueState<String> isNewState;

                    //初始化状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isNewState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<String>("isNew-state", String.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, Context ctx, Collector<JSONObject> out) throws Exception {

                        //取出数据中"is_new"字段
                        String isNew = jsonObject.getJSONObject("common")
                                .getString("is_new");

                        //如果isNew为1,则需要继续校验
                        if ("1".equals(isNew)) {
                            //取出状态中的数据,并判断是否为null
                            if (isNewState.value() != null) {
                                //说明当前mid不是新用户,修改is_new的值
                                jsonObject.getJSONObject("common").put("is_new", "0");
                            } else {
                                //说明为真正的新用户
                                isNewState.update("0");
                            }
                        }

                        //输出数据
                        out.collect(jsonObject);
                    }
                });
        //打印测试
        //jsonObjWithNewFlag.print();

        //TODO 5.使用侧输出流将 启动、曝光、页面数据分流
        OutputTag<String> startOutPutTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context ctx, Collector<String> out) throws Exception {

                //获取启动数据
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    //为启动数据
                    ctx.output(startOutPutTag, jsonObject.toJSONString());
                } else {
                    //不是启动数据,则一定为页面数据
                    out.collect(jsonObject.toJSONString());

                    //获取曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");

                    //取出公共字段、页面信息、时间戳
                    JSONObject common = jsonObject.getJSONObject("common");
                    JSONObject page = jsonObject.getJSONObject("page");
                    Long ts = jsonObject.getLong("ts");

                    //判断曝光数据是否存在
                    if (displays != null && displays.size() > 0) {

                        JSONObject displayObj = new JSONObject();
                        displayObj.put("common", common);
                        displayObj.put("page", page);
                        displayObj.put("ts", ts);

                        //遍历每一个曝光信息
                        for (Object display : displays) {
                            displayObj.put("display", display);
                            //输出到侧输出流
                            ctx.output(displayOutputTag, displayObj.toJSONString());
                        }
                    }
                }
            }
        });

        //TODO 6.将三个流的数据写入Kafka
        jsonObjDS.getSideOutput(dirty).print("Dirty>>>>>>>>>>>");
        pageDS.print("Page>>>>>>>>>>>");
        pageDS.getSideOutput(startOutPutTag).print("Start>>>>>>>>>>>>");
        pageDS.getSideOutput(displayOutputTag).print("Display>>>>>>>>>>>>>");

        String pageSinkTopic = "dwd_page_log";
        String startSinkTopic = "dwd_start_log";
        String displaySinkTopic = "dwd_display_log";
        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(pageSinkTopic));
        pageDS.getSideOutput(startOutPutTag).addSink(MyKafkaUtil.getFlinkKafkaProducer(startSinkTopic));
        pageDS.getSideOutput(displayOutputTag).addSink(MyKafkaUtil.getFlinkKafkaProducer(displaySinkTopic));

        //TODO 7.启动
        env.execute();

    }
}
