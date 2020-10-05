package com.pgman.goku.datastream.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CustomKakfaSinkByString {

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

        DataStream<String> words = kafkaString.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String word : s.split(" ")) {
                    collector.collect(word);
                }
            }
        });

        // 数据写入到kafka
        words.addSink(new RichSinkFunction<String>() {

            private Producer<String,String> producer;

            // 程序首次执行时访问
            @Override
            public void open(Configuration parameters) throws Exception {

                super.open(parameters);
                if (this.producer == null){
                    Properties properties = new Properties();
                    properties.put("bootstrap.servers", "test.server:9092");
                    properties.put("acks", "all");
                    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                    this.producer = new KafkaProducer<>(properties);
                }

            }

            @Override
            public void invoke(String value, Context context) throws Exception {
                producer.send(new ProducerRecord<>("test",  value));
            }
        });


        env.execute("KafkaRichSinkByString");

    }
}
