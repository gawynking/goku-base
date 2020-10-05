package com.pgman.goku.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerPartitionManualCommitDemo {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "test.server:9092");
        props.put("group.id", "pgman-test99990000");
        props.put("enable.auto.commit", "false");// 关闭自动提交
        props.put("auto.offset.reset", "earliest"); // 从最早开始消费
        props.put("auto.commit.interval.ms", "100000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

                    for (ConsumerRecord<String, String> record : partitionRecords) {

                        System.out.println(record.offset() + ":" + record.value());

                    }

                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));

                }

            }
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }

}
