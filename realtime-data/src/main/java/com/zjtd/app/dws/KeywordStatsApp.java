package com.zjtd.app.dws;

import com.zjtd.app.func.KeywordTableFunction;
import com.zjtd.bean.KeywordStats;
import com.zjtd.utils.ClickHouseUtil;
import com.zjtd.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Desc: 搜索关键字计算
 */
//数据流 Web/App -> Nginx -> SpringBoot -> Kafka      -> Flink -> Kafka     -> Flink -> ClickHouse
//进程   MockLog -> Nginx -> Logger.sh  -> Kafka(ZK)  -> BaseLogApp -> Kafka -> KeywordStatsApp -> ClickHouse
public class KeywordStatsApp {

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
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL方式读取Kafka数据创建表
        String topic = "dwd_page_log";
        String groupId = "keyword_stats_app_201109";

        tableEnv.executeSql("create table page_view( " +
                "common Map<String,String>, " +
                "page Map<String,String>, " +
                "ts bigint, " +
                "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND" +
                ")with(" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")");

        //打印测试
//        tableEnv.executeSql("select * from page_view")
//                .print();

        //TODO 3.过滤数据
        Table fullWordTable = tableEnv.sqlQuery("" +
                "select " +
                "    page['item'] fullWord, " +
                "    rowtime " +
                "from page_view " +
                "where page['item_type'] = 'keyword' " +
                "and page['last_page_id'] = 'search' " +
                "and page['item'] is not null");

        //打印测试
        //tableEnv.toAppendStream(fullWordTable, Row.class).print();

        //TODO 4.注册UDTF函数并进行分词处理
        tableEnv.createTemporarySystemFunction("SplitFunction", KeywordTableFunction.class);
        Table splitWordTable = tableEnv.sqlQuery("SELECT word, rowtime FROM " + fullWordTable + ", LATERAL TABLE(SplitFunction(fullWord))");

        //打印测试
        //tableEnv.toAppendStream(splitWordTable, Row.class).print();

        //TODO 5.开窗、分组、聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) ct, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from  " + splitWordTable +
                " group by word, " +
                "  TUMBLE(rowtime, INTERVAL '10' SECOND)");

        //TODO 6.转换为流并写入ClickHouse
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);
        keywordStatsDataStream.print();
        keywordStatsDataStream.addSink(ClickHouseUtil.getSinkFunc("insert into keyword_stats_201109(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //TODO 7.启动
        env.execute();

    }

}

