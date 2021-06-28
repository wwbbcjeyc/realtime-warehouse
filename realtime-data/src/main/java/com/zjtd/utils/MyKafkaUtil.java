package com.zjtd.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {

    private static Properties properties = new Properties();

    private static String DEFAULT_TOPIC = "dwd_default_topic";

    static {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.104.241.103:9092");
    }

    public static <T> FlinkKafkaProducer<T> getFlinkKafkaProducerBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * 获取生产者对象
     *
     * @param topic 主题
     */
    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {

        return new FlinkKafkaProducer<String>(topic,
                new SimpleStringSchema(),
                properties);
    }

    /**
     * 获取消费者对象
     *
     * @param topic   主题
     * @param groupId 消费者组
     */
    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String groupId) {

        //添加消费组属性
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(topic,
                new SimpleStringSchema(),
                properties);
    }

    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        return "'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = 'hadoop102:9092', " +
                " 'properties.group.id' = '" + groupId + "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'";
    }

}
