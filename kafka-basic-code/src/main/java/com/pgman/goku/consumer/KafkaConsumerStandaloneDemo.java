package com.pgman.goku.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 *  独立分配模式
 */
public class KafkaConsumerStandaloneDemo {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "test.server:9092");
        props.put("group.id", "pgman-test");
        props.put("enable.auto.commit", "false");// 关闭自动提交
        props.put("auto.offset.reset", "earliest"); // 从最早开始消费
        props.put("auto.commit.interval.ms", "100000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        List<TopicPartition> partitions = new ArrayList<TopicPartition>();
        List<PartitionInfo> allPartitions = consumer.partitionsFor("test");

        if (allPartitions != null && !allPartitions.isEmpty()) {

            for (PartitionInfo partitioninfo : allPartitions) {

                partitions.add(new TopicPartition(partitioninfo.topic(), partitioninfo.partition()));

            }
            consumer.assign(partitions);

        }

        try {

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset=%d, key=%s, value=%s%n", record.offset(), record.key(), record.value());
                    consumer.commitSync();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.commitSync();
            consumer.close();
        }

    }

}
