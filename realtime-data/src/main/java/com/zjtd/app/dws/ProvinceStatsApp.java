package com.zjtd.app.dws;

import com.zjtd.bean.ProvinceStats;
import com.zjtd.utils.ClickHouseUtil;
import com.zjtd.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//数据  Web/App -> Nginx -> SpringBoot -> Mysql(Binlog) -> FlinkApp -> Kafka(ods) -> Flink -> Kafka(dwd)/HBase(DIM) -> Flink -> Kafka(dwm) -> FlinkApp -> ClickHouse

//进程     Mock -> Mysql(Binlog) -> FlinkCDC -> Kafka(ods_base_db) -> BaseDBApp -> Kafka(dwd_*) -> OrderWideApp(Redis) -> Kafka(dwm_order_wide) -> ProvinceStatsApp -> ClickHouse
public class ProvinceStatsApp {

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

        //TODO 2.使用DDL的方式创建表,提取时间戳字段生成WaterMark
        String topic = "dwm_order_wide";
        String groupId = "province_stats_app_1109";
        tableEnv.executeSql("create table order_wide( " +
                "order_id bigint, " +
                "province_id bigint, " +
                "province_name string, " +
                "province_area_code string, " +
                "province_iso_code string, " +
                "province_3166_2_code string, " +
                "total_amount decimal, " +
                "create_time string, " +
                "rowtime as TO_TIMESTAMP(create_time), " +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND " +
                ") WITH(" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")");

        //测试打印
//        tableEnv.executeSql("select * from order_wide")
//                .print();

        //TODO 3.执行查询 开窗、分组、聚合
        Table tableResult = tableEnv.sqlQuery("select  " +
                "    DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt,  " +
                "    DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt,  " +
                "    province_id,  " +
                "    province_name,  " +
                "    province_area_code,  " +
                "    province_iso_code,  " +
                "    province_3166_2_code,  " +
                "    sum(total_amount) order_amount,  " +
                "    count(distinct order_id) order_count,  " +
                "    UNIX_TIMESTAMP()*1000 ts  " +
                "from order_wide  " +
                "group by   " +
                "    province_id,  " +
                "    province_name,  " +
                "    province_area_code,  " +
                "    province_iso_code,  " +
                "    province_3166_2_code,  " +
                "    TUMBLE(rowtime, INTERVAL '10' SECOND)");

        //TODO 4.将查询结果的动态表转换为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(tableResult, ProvinceStats.class);
        provinceStatsDataStream.print(">>>>>>>>>>>>");

        //TODO 5.将数据写入ClickHouse
        provinceStatsDataStream.addSink(ClickHouseUtil.getSinkFunc("insert into province_stats_201109 values(?,?,?,?,?,?,?,?,?,?)"));

        //TODO 6.启动
        env.execute();

    }

}
