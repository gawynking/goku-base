package com.pgman.goku.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class KafkaUtils {

    /**
     * 设置默认配置信息
     *
     * @param bootstrapServers
     * @return
     */
    public static Properties getProducerProperties(String bootstrapServers){

        Properties properties = new Properties();

        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("acks", "1");
        //properties.put("acks", "all");
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 10);
        properties.put("buffer.memory", 33554432);
        properties.put("max.block.ms", 3000);
        properties.put("max.request.size", 10485760);
        properties.put("request.timeout.ms", 60000);
        properties.put("block.on.buffer.full", "true");
        properties.put("compressiont.typ", "snappy");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return properties;

    }

    /**
     * 消费者参数设置
     *
     * @param bootstrapServers
     * @return
     */
    public static Properties getConsumerProperties(String bootstrapServers){

        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", "pgman-test");
        properties.put("enable.auto.commit", "false");// 关闭自动提交
        properties.put("auto.offset.reset", "earliest"); // 从最早开始消费
        properties.put("auto.commit.interval.ms", "100000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return properties;

    }

    /**
     * 获得生产者客户端
     *
     * @param properties
     * @return
     */
    public static Producer<?, ?> getProducer(Properties properties){
        return new KafkaProducer(properties);
    }

}
