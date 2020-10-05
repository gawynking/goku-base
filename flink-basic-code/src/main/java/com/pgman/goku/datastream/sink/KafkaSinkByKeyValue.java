package com.pgman.goku.datastream.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaSinkByKeyValue {

    public static void main(String[] args) throws Exception{

        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        String topicName = "pgman";
        String groupId = "flink-pgman-test-kafka-string";

        // 1 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 定义kafka参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "test.server:9092");
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 注册kafka数据源
        DataStreamSource<String> kafkaString = env.addSource(new FlinkKafkaConsumer011<String>(topicName, new SimpleStringSchema(), properties));

        DataStream<Tuple2<String, Integer>> wordCount = kafkaString.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String word : s.split(" ")) {
                    collector.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        })
                .keyBy(0)
                .sum(1);


        Properties producer = new Properties();
        producer.put("bootstrap.servers", "test.server:9092");
        producer.put("acks", "all");
        producer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        wordCount.addSink(new FlinkKafkaProducer011<Tuple2<String, Integer>>(
                "test",
                new KeyedSerializationSchema<Tuple2<String, Integer>>() {
                    @Override
                    public byte[] serializeKey(Tuple2<String, Integer> data) {
                        return data.f0.getBytes();
                    }

                    @Override
                    public byte[] serializeValue(Tuple2<String, Integer> data) {
                        return String.valueOf(data.f1).getBytes();
                    }

                    @Override
                    public String getTargetTopic(Tuple2<String, Integer> data) {
                        return "test";
                    }
                },
                producer,
                FlinkKafkaProducer011.Semantic.EXACTLY_ONCE // 仅且一次语义
        ));

        env.execute("KafkaSinkByKeyValue");

    }

}
