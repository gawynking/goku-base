package com.pgman.goku.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerManualCommitDemo {

    public static void main(String[] args) {

        String topicName = "ck-test";
        String groupId = "ck-test002";

        Properties props = new Properties();
        props.put("bootstrap.servers", "test.server:9092");
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");// 关闭自动提交
//        props.put("auto.offset.reset", "earliest"); // 从最早开始消费
//        props.put("auto.offset.reset", "latest"); // 从最新处开始消费
//        props.put("auto.commit.interval.ms", "100000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topicName));

        try {

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset=%d, key=%s, value=%s%n", record.offset(), record.key(), record.value());
//                    consumer.commitAsync();// 异步提交

                    consumer.commitSync(); // 同步提交位移
                }
            }

        }catch(Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }

}
