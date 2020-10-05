package com.pgman.goku.datastream.transformation;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 为每个输入元素应用判断，返回结果为真的数据,经过算子返回的数据不变
 *
 */
public class FilterExample {

    public static void main(String[] args) {

        String groupId = "filter-order";

//        filter(groupId);

        filterWithRichFunction(groupId);

    }


    /**
     * 应用 FilterFunction
     * @param groupId
     */
    public static void filter(String groupId) {

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

        SingleOutputStreamOperator<String> filterDataStream = jsonText.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return 4 == json.getInteger("order_status");
            }
        });


        filterDataStream.print("过滤后的数据");

        try {
            env.execute("filter");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 应用 RichFilterFunction
     * @param groupId
     */
    public static void filterWithRichFunction(String groupId) {

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

        SingleOutputStreamOperator<String> filterDataStream = jsonText.filter(new RichFilterFunction<String>() {

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
            public boolean filter(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return 4 == json.getInteger("order_status");
            }

        });


        filterDataStream.print("过滤后的数据");

        try {
            env.execute("filter");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}