package com.pgman.goku.datastream.transformation;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;


/**
 * 对于输入的每一个元素，输出0个、1个或多个元素，flatMap算子可以用来替代map算子，功能更强大，应用范围更广泛。
 */
public class FlatMapExample {

    public static void main(String[] args) {

        String groupId = "flatMap-order";

//        flatMap(groupId);

        flatMapWithRichFunction(groupId);

    }


    /**
     * 应用 FlatMapFunction
     * @param groupId
     */
    public static void flatMap(String groupId){

        // 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 定义kafka参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConfigurationManager.getString("broker.list"));
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");



        // 注册kafka数据源
        DataStreamSource<String> jsonText = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        SingleOutputStreamOperator<String> flatMapDataStream = jsonText.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String text, Collector<String> collector) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                if(4 == json.getInteger("order_status")){
                    collector.collect(json.toJSONString());
                }
            }

        });


        flatMapDataStream.print("已完成订单");

        try {
            env.execute("flatMap");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 应用
     *
     * @param groupId
     */
    public static void flatMapWithRichFunction(String groupId){

        // 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 定义kafka参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConfigurationManager.getString("broker.list"));
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");



        // 注册kafka数据源
        DataStreamSource<String> jsonText = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        SingleOutputStreamOperator<String> flatMapDataStream = jsonText.flatMap(new RichFlatMapFunction<String, String>() {

            String tag = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                tag = "RichFunction";
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void flatMap(String text, Collector<String> collector) throws Exception {

                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                if(4 == json.getInteger("order_status")){
                    json.put("tag",tag);
                    collector.collect(json.toJSONString());
                }

            }
        });


        flatMapDataStream.print("已完成订单");

        try {
            env.execute("flatMap");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
