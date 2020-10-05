package com.pgman.goku.datastream.transformation;


import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 应用在 KeyedStream 上 ，将当前元素与最后一个Reduce的值组合并产生新值。
 */
public class ReduceExample {


    public static void main(String[] args) {

        String groupId = "reduce-order";

//        reduce(groupId);

        reduceWithRichFunction(groupId);

    }


    /**
     * 全局reduce使用方法，直接在keyBy之后，未指定窗口
     *
     * @param groupId
     */
    public static void reduce(String groupId){

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

        SingleOutputStreamOperator<Tuple2<String, Integer>> keyedDataStream = jsonText.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple2<>(json.getString("customer_id"), json.getInteger("order_amt"));
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1.f0,t1.f1 + t2.f1);
            }
        });

        keyedDataStream.print("客户下单总金额");

        try {
            env.execute("keyedWithTuple");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     *
     *
     * @param groupId
     */
    public static void reduceWithRichFunction(String groupId){

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

        SingleOutputStreamOperator<Tuple2<String, Integer>> keyedDataStream = jsonText.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple2<>(json.getString("customer_id"), json.getInteger("order_amt"));
            }
        }).keyBy(0).reduce(new RichReduceFunction<Tuple2<String, Integer>>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1.f0,t1.f1 + t2.f1);
            }
        });

        keyedDataStream.print("客户下单总金额");

        try {
            env.execute("keyedWithTuple");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
