package com.pgman.goku.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerDemo {

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

        String topicName = "api_topic";
        String groupId = "ck-test00200908kkk";

        Properties props = new Properties();
        props.put("bootstrap.servers", "test.server:9092");
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");// 自动提交
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest"); // 从最早开始消费

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
//                    System.out.printf("offset=%d, key=%s, value=%s%n", record.offset(), record.key(), record.value());

                    System.out.println(record.value());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }

    }
}
