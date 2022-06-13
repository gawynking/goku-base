package com.pgman.goku.datastream.transformation;


import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.pojo.Order;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 滚动聚合数据流上的聚合。min和minBy之间的差异是min返回最小值，而minBy返回该字段中具有最小值的元素（max和maxBy相同）。
 *
 * keyedStream.sum(0);
 * keyedStream.sum("key");
 * keyedStream.min(0);
 * keyedStream.min("key");
 * keyedStream.max(0);
 * keyedStream.max("key");
 * keyedStream.minBy(0);
 * keyedStream.minBy("key");
 * keyedStream.maxBy(0);
 * keyedStream.maxBy("key");
 *
 * min():获取的最小值，指定的field是最小，但不是最小的那条记录，后面的示例会清晰的显示。min进替换了最小值字段
 * minBy():获取的最小值，同时也是最小值的那条记录。minBy返回最小值整行记录
 * max()与maxBy()的区别也是一样。
 *
 */
public class AggregationsExample {

    public static void main(String[] args) {

        String groupId = "aggregations-order";

//        aggregations(groupId);

        aggregationsWithPOJO(groupId);


    }


    /**
     *
     * @param groupId
     */
    public static void aggregations(String groupId){

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

        KeyedStream<Tuple3<String, Integer, Integer>, Tuple> keyedDataStream = jsonText.map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getString("customer_id"), json.getInteger("order_status"), json.getInteger("order_amt"));
            }
        }).keyBy(0);

        // sum
        keyedDataStream.sum(2).print("sum-index");

        // min
//        keyedDataStream.min(2).print("min-index");

//        // max
//        keyedDataStream.max(2).print("max-index");

//        // maxBy
//        keyedDataStream.maxBy(2).print("maxBy-index");

//        // minBy
//        keyedDataStream.minBy(1).print("minBy-index");

        try {
            env.execute("fold");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     *
     * @param groupId
     */
    public static void aggregationsWithPOJO(String groupId){

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

        KeyedStream<Order, Tuple> keyedDataStream = jsonText.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String s) throws Exception {
                Order order = new Order();
                JSONObject json = JSONUtils.getJSONObjectFromString(s);

                order.setId(json.getInteger("id"));
                order.setCustomerId(json.getInteger("customer_id"));
                order.setOrderAmt(json.getInteger("order_amt"));
                order.setOrderStatus(json.getInteger("order_status"));
                order.setCreateTime(json.getString("create_time"));

                return order;
            }
        }).keyBy("customerId");

        // sum
//        keyedDataStream.sum("orderAmt").print("sum-index");

        // min
        keyedDataStream.min("orderAmt").print("min-index");

        // minBy
        keyedDataStream.minBy("orderAmt").print("minBy-index");

//        // max
//        keyedDataStream.max("orderAmt").print("max-index");

//        // maxBy
//        keyedDataStream.maxBy("orderAmt").print("maxBy-index");



        try {
            env.execute("fold");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
