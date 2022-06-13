package com.pgman.goku.datastream.transformation;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.pojo.Order;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;


/**
 * 分流 ： 将输入数据按照key键 分流成不同分区，返回KeyedStream，keyBy指定键有多重方式
 */
public class KeyedExample {

    public static void main(String[] args) {

        String groupId = "keyed-order";

//        keyedWithTupleIndex(groupId);

//        keyedWithTupleColumnName(groupId);

//        keyedWithKeySelector(groupId);

//        keyedWithScala(groupId);

//        keyedWithMultiFieldAndReduce(groupId)

        keyedWithMultiFieldAndTuple(groupId);

    }



    /**
     * 求客户累积下单总金额
     *
     * 为Tupe结构 索引 定义键
     *
     * @param groupId
     */
    public static void keyedWithTupleIndex(String groupId){

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
        }).keyBy(0).sum(1);

        keyedDataStream.print("客户下单总金额");

        try {
            env.execute("keyedWithTuple");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 统计用户下单总金额
     *
     * 通过POJO对象方式 引用字段名 指定键
     *
     * @param groupId
     */
    public static void keyedWithTupleColumnName(String groupId){

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

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> keyedDataStream = jsonText.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);

                Order order = new Order();

                order.setId(json.getInteger("id"));
                order.setCustomerId(json.getInteger("customer_id"));
                order.setOrderAmt(json.getInteger("order_amt"));
                order.setOrderStatus(json.getInteger("order_status"));
                order.setCreateTime(json.getString("create_time"));


                return order;
            }
        }).keyBy("customerId").sum("orderAmt").flatMap(new FlatMapFunction<Order, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(Order order, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                if (order.getCustomerId() <= 10) {
                    collector.collect(new Tuple2<>(order.getCustomerId(), order.getOrderAmt()));
                }
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
     * 使用key选择器指定键
     *
     * 测试使用key选择器 指定分流键 不能使用聚合算子 sum max min等等 ，需要使用reduce手写处理逻辑
     *
     * @param groupId
     */
    public static void keyedWithKeySelector(String groupId){

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

        SingleOutputStreamOperator<JSONObject> KeyedDataStream = jsonText.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String text, Collector<JSONObject> collector) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                JSONObject result = new JSONObject();

                result.put("customer_id",json.getInteger("customer_id"));
                result.put("order_amt",json.getInteger("order_amt"));

                collector.collect(result);
            }
        }).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject json) throws Exception {
                return json.getString("customer_id");
            }
        }).reduce(new ReduceFunction<JSONObject>() {
            @Override
            public JSONObject reduce(JSONObject t1, JSONObject t2) throws Exception {
                JSONObject json = new JSONObject();

                json.put("customer_id",t1.getInteger("customer_id"));
                json.put("order_amt",t1.getInteger("order_amt") + t2.getInteger("order_amt"));
                return json;
            }
        });

        KeyedDataStream.print("客户总下单金额");

        try {
            env.execute("keyed");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 使用scala简写指定键
     *
     * 测试使用scala简写 指定分流键 不能使用聚合算子 sum max min等等 ，需要使用reduce手写处理逻辑
     * @param groupId
     */
    public static void keyedWithScala(String groupId){

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

        SingleOutputStreamOperator<JSONObject> KeyedDataStream = jsonText.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String text, Collector<JSONObject> collector) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                JSONObject result = new JSONObject();

                result.put("customer_id",json.getInteger("customer_id"));
                result.put("order_amt",json.getInteger("order_amt"));

                collector.collect(result);
            }
        }).keyBy(json -> json.getInteger("customer_id")).reduce(new ReduceFunction<JSONObject>() {
            @Override
            public JSONObject reduce(JSONObject t1, JSONObject t2) throws Exception {
                JSONObject json = new JSONObject();

                json.put("customer_id",t1.getInteger("customer_id"));
                json.put("order_amt",t1.getInteger("order_amt") + t2.getInteger("order_amt"));
                return json;
            }
        });

        KeyedDataStream.print("客户总下单金额");

        try {
            env.execute("keyed");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 多字段分流
     *
     * @param groupId
     */
    public static void keyedWithMultiFieldAndReduce(String groupId){

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

        SingleOutputStreamOperator<JSONObject> KeyedDataStream = jsonText.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String text, Collector<JSONObject> collector) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                JSONObject result = new JSONObject();

                result.put("customer_id", json.getInteger("customer_id"));
                result.put("order_status", json.getInteger("order_status"));
                result.put("order_amt", json.getInteger("order_amt"));

                collector.collect(result);
            }
        }).keyBy(new KeySelector<JSONObject, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> getKey(JSONObject jsonObject) throws Exception {
                return new Tuple2<>(jsonObject.getInteger("customer_id"), jsonObject.getInteger("order_status"));
            }
        }).reduce(new ReduceFunction<JSONObject>() {
            @Override
            public JSONObject reduce(JSONObject t1, JSONObject t2) throws Exception {

                JSONObject json = new JSONObject();
                json.put("customer_id",t1.getInteger("customer_id"));
                json.put("order_status",t1.getInteger("order_status"));

                json.put("order_amt",t1.getInteger("order_amt") + t2.getInteger("order_amt"));

                return json;
            }
        });


        KeyedDataStream.print("客户总下单金额");

        try {
            env.execute("keyed");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 多字段分组 基于Tuple3
     *
     * @param groupId
     */
    public static void keyedWithMultiFieldAndTuple(String groupId){

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

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> KeyedDataStream = jsonText.flatMap(new FlatMapFunction<String, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public void flatMap(String text, Collector<Tuple3<Integer, Integer, Integer>> collector) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                if(json.getInteger("order_status") >= 3 && json.getInteger("customer_id") <= 5) {
                    collector.collect(new Tuple3<>(json.getInteger("customer_id"), json.getInteger("order_status"), json.getInteger("order_amt")));
                }
            }
        }).keyBy(0, 1).sum(2);


        KeyedDataStream.print("客户总下单金额");

        try {
            env.execute("keyed");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
