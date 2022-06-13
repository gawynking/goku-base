package com.pgman.goku.datastream.transformation;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 连接流及其CoMap, CoFlatMap算子示例
 *
 * connect : “连接”两个保留类型的数据流。连接允许在两个流之间共享状态。
 *
 * CoMap, CoFlatMap : 类似于 应用在 ConnectedStreams 上的 map 和 flatMap 算子 ，connect算子之后必须接其做后续处理
 *
 */
public class ConnectExample {

    public static void main(String[] args) {

        String groupId = "connect-order";

//        connectWithCoMap(groupId);

//        connectWithCoMapAndRichFunction(groupId);

//        connectWithCoFlatMap(groupId);

        connectWithCoFlatMapAndRichFunction(groupId);


    }



    /**
     * connect 及 CoMap RichFunction 操作
     * @param groupId
     */
    public static void connectWithCoMapAndRichFunction(String groupId){

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
        // 1 订单数据源
        DataStreamSource<String> jsonText1 = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        // 2 订单明细数据源
        DataStreamSource<String> jsonText2 = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.detail.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        // connect 算子操作
        ConnectedStreams<String, String> connectStream = jsonText1.connect(jsonText2);

        // 针对 connectStream 上的 CoMap操作
        SingleOutputStreamOperator<JSONObject> coMapResult = connectStream.map(new RichCoMapFunction<String, String, JSONObject>() {

            String tag = "null";

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                tag = "rich-function";
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public JSONObject map1(String value) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(value);
                json.put("tag",tag);
                return json;
            }

            @Override
            public JSONObject map2(String value) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(value);
                json.put("tag",tag);
                return json;
            }
        });

        coMapResult.print("comap");


        try {
            env.execute("connect");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * connect 及 CoMap 操作
     * @param groupId
     */
    public static void connectWithCoMap(String groupId){

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
        // 1 订单数据源
        DataStreamSource<String> jsonText1 = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        // 2 订单明细数据源
        DataStreamSource<String> jsonText2 = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.detail.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        // connect 算子操作
        ConnectedStreams<String, String> connectStream = jsonText1.connect(jsonText2);

        // 针对 connectStream 上的 CoMap操作
        SingleOutputStreamOperator<JSONObject> coMapResult = connectStream.map(new CoMapFunction<String, String, JSONObject>() {
            @Override
            public JSONObject map1(String value) throws Exception {
                return JSONUtils.getJSONObjectFromString(value);
            }

            @Override
            public JSONObject map2(String value) throws Exception {
                return JSONUtils.getJSONObjectFromString(value);
            }
        });

        coMapResult.print("comap");


        try {
            env.execute("connect");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    /**
     * connect 及 CoFlatMap 操作
     * @param groupId
     */
    public static void connectWithCoFlatMap(String groupId){

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
        // 1 订单数据源
        DataStreamSource<String> jsonText1 = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        // 2 订单明细数据源
        DataStreamSource<String> jsonText2 = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.detail.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        // connect 算子操作
        ConnectedStreams<String, String> connectStream = jsonText1.connect(jsonText2);

        // 针对 connectStream 上的 CoMap操作
        SingleOutputStreamOperator<JSONObject> coFlatMapResult = connectStream.flatMap(new CoFlatMapFunction<String, String, JSONObject>() {

            @Override
            public void flatMap1(String value, Collector<JSONObject> out) throws Exception {
                out.collect(JSONUtils.getJSONObjectFromString(value));
            }

            @Override
            public void flatMap2(String value, Collector<JSONObject> out) throws Exception {
                out.collect(JSONUtils.getJSONObjectFromString(value));
            }
        });

        coFlatMapResult.print("coflatmap");


        try {
            env.execute("connect");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * connect 及 CoFlatMap RichFunction 操作
     * @param groupId
     */
    public static void connectWithCoFlatMapAndRichFunction(String groupId){

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
        // 1 订单数据源
        DataStreamSource<String> jsonText1 = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        // 2 订单明细数据源
        DataStreamSource<String> jsonText2 = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.detail.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        // connect 算子操作
        ConnectedStreams<String, String> connectStream = jsonText1.connect(jsonText2);

        // 针对 connectStream 上的 CoMap操作
        SingleOutputStreamOperator<JSONObject> coFlatMapResult = connectStream.flatMap(new RichCoFlatMapFunction<String, String, JSONObject>() {

            String tag = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                tag = "rich-function";
            }

            @Override
            public void flatMap1(String value, Collector<JSONObject> out) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(value);
                json.put("tag",tag);
                out.collect(json);
            }

            @Override
            public void flatMap2(String value, Collector<JSONObject> out) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(value);
                json.put("tag",tag);
                out.collect(json);
            }
        });

        coFlatMapResult.print("coflatmap");


        try {
            env.execute("connect");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
