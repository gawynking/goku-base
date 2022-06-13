package com.pgman.goku.datastream.transformation;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 针对每一个输入元素应用生成另一个一个元素。
 *
 */
public class MapExample {


    public static void main(String[] args) {

        String groupId = "order-test-00";

//        map(groupId);

        mapWithRichFunction(groupId);

    }


    /**
     * 基于 MapFunction 的map示例
     *
     * @param groupId
     */
    public static void map(String groupId){

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

        SingleOutputStreamOperator<String> mapDataStream = jsonText.map(new MapFunction<String, String>() {

            @Override
            public String map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                Integer orderAMT = json.getInteger("order_amt") * 2;
                json.put("order_amt",orderAMT);

                return json.toJSONString();

            }

        });

        mapDataStream.print("全部订单");

        try {
            env.execute("map");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 应用 RichMapFunction 函数
     * @param groupId
     */
    public static void mapWithRichFunction(String groupId){

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

        SingleOutputStreamOperator<String> mapDataStream = jsonText.map(new RichMapFunction<String, String>() {

            String tag = null;

            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
            }

            @Override
            public RuntimeContext getRuntimeContext() {
                return super.getRuntimeContext();
            }

            @Override
            public IterationRuntimeContext getIterationRuntimeContext() {
                return super.getIterationRuntimeContext();
            }

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
            public String map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                json.put("tag",tag);
                return json.toJSONString();
            }


        });

        mapDataStream.print("全部订单");

        try {
            env.execute("map");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
