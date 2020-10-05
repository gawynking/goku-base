package com.pgman.goku.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerNolossDemo {

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

        String topicName = "ck-test";

        Properties props = new Properties();

        props.put("bootstrap.servers", "test.server:9092");

        // Kafka Producer无丢失异步参数配置
        props.put("block.on.buffer.full", "true");
        props.put("acks", "all");
        props.put("retries", Integer.MAX_VALUE);
        props.put("max.in.flight.requests.per.connection", "1");
        props.put("unclean.leader.election.enable", "false");
        props.put("replication.factor", "3");
        props.put("min.insync.relicas", "2");
        props.put("replicacation.factor", "3"); // 参数配置必须保证 ： replicacation.factor > min.insync.relicas
        props.put("enable.auto.commit", "false");


        /**
         *  Kafka Producer无丢失异步发送 broker 端配置：
         *
         *  unclean.leader.election.enable = false
         *  replication.factor >= 3
         *  min.insync.replicas > 1
         *  replicacation.factor > min.insync.relicas
         *
         */

        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);
        props.put("max.request.size", 10485760);
        props.put("request.timeout.ms", 60000);
        props.put("block.on.buffer.full", "true");
        props.put("compressiont.typ", "snappy");

        Serializer<String> keySerializer = new StringSerializer();
        Serializer<String> valueSerializer = new StringSerializer();

        Producer<String, String> producer = new KafkaProducer(props, keySerializer, valueSerializer);

        int i = 0;
        boolean flag = true;

        while (flag) {
            // 同步发送
            // producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i))).get();

            i++;
            Thread.sleep(1000);
            // 带回调函数的生产者程序
            producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)), new Callback() {

                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // TODO Auto-generated method stub

                    if (exception == null) {
                        // 消息发送成功逻辑
                        System.out.println("发送消息topic:" + metadata.topic() + " partition:" + metadata.partition() + " offset:" + metadata.offset());

                    } else {
                        if (exception instanceof RetriableException) {
                            // 处理可重试瞬时异常逻辑
                            System.out.println("处理可重试瞬时异常");
                        } else {
                            // 处理不可重试异常逻辑
                            System.out.println("处理不可重试异常");
                        }
                    }

                }
            });

        }
        producer.close();

    }


}
