package com.zjtd.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MyDeserializerFunc implements DebeziumDeserializationSchema<String> {
    /**
     * {
     * "data":"{"id":11,"tm_name":"sasa"}",
     * "db":"",
     * "tableName":"",
     * "op":"c u d",
     * "ts":""
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        //获取主题信息,提取数据库和表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String db = fields[1];
        String tableName = fields[2];

        //获取Value信息,提取数据本身
        Struct value = (Struct) sourceRecord.value();
        Struct after = value.getStruct("after");
        JSONObject jsonObject = new JSONObject();
        if (after != null) {
            for (Field field : after.schema().fields()) {
                Object o = after.get(field);
                jsonObject.put(field.name(), o);
            }
        }

        //获取Value信息,提取删除或者修改的数据本身
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            for (Field field : before.schema().fields()) {
                Object o = before.get(field);
                beforeJson.put(field.name(), o);
            }
        }

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        //创建结果JSON
        JSONObject result = new JSONObject();
        result.put("database", db);
        result.put("table", tableName);
        result.put("data", jsonObject);
        result.put("before-data", beforeJson);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }
        result.put("type", type);

        //输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}

