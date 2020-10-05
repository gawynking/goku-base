package com.pgman.goku.datastream.transformation;


import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * split 和 select 算子 实现流切割 示例
 *
 * split : 根据某些标准将流拆分为两个或更多个流。
 *
 * select : 从拆分流中选择一个或多个流。
 *
 */
public class SplitExample {

    public static void main(String[] args) {

        String groupId = "split-order";

        split(groupId);

    }


    public static void split(String groupId){

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

        SplitStream<JSONObject> split = jsonText.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONUtils.getJSONObjectFromString(s);
            }
        }).split(new OutputSelector<JSONObject>() {
            @Override
            public Iterable<String> select(JSONObject value) {

                List<String> output = new ArrayList<String>();

                if (value.getInteger("order_status") == 4) {
                    output.add("success");
                } else {
                    output.add("no success");
                }

                return output;
            }
        });


        DataStream<JSONObject> success = split.select("success");
        DataStream<JSONObject> no_success = split.select("no success");
        DataStream<JSONObject> all = split.select("success", "no success");

        success.print("已完成订单");
        no_success.print("未完成订单");
        all.print("全部订单");


        try {
            env.execute("map");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
