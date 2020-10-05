package com.pgman.goku.datastream.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class TimeCharacteristicExample {


    public static void main(String[] args) {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 处理时间 语义
        // env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 设置 提取事件 语义
        // env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        // 设置 事件时间 语义
         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



        // 定义kafka参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "test.server:9092");
        properties.put("group.id", "test999");
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");



        // 注册kafka数据源
        DataStreamSource<String> eventDataStream = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        "pgman",
                        new SimpleStringSchema(),
                        properties
                )
        );






    }
}
