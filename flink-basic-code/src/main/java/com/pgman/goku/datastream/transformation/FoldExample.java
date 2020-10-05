package com.pgman.goku.datastream.transformation;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFoldFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 *
 * 折叠函数，当应用于序列（1,2,3,4,5）时，发出序列“start-1”，“start-1-2”，“start-1-2-3”,. ..
 *
 * 具有初始值的键控数据流上的“滚动”计算。将当前元素与最后折叠的值组合并生成新值。已废弃
 *
 */
public class FoldExample {

    public static void main(String[] args) {

        String groupId = "fold-order";

//        fold(groupId);

        foldWithRichFunction(groupId);

    }

    /**
     *
     * @param groupId
     */
    public static void fold(String groupId){

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

        SingleOutputStreamOperator<String> keyedDataStream = jsonText.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple2<>(json.getString("customer_id"), json.getInteger("order_amt"));
            }
        }).keyBy(0).fold("start", new FoldFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String fold(String current, Tuple2<String, Integer> o) throws Exception {
                return current + "-" + o.f1;
            }
        });

        keyedDataStream.print("fold");

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
    public static void foldWithRichFunction(String groupId){

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

        SingleOutputStreamOperator<Tuple2<String, String>> keyedDataStream = jsonText.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple2<>(json.getString("customer_id"), json.getInteger("order_amt"));
            }
        }).keyBy(0).fold(new Tuple2<>("", "start"), new RichFoldFunction<Tuple2<String, Integer>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> fold(Tuple2<String, String> current, Tuple2<String, Integer> o) throws Exception {
                return new Tuple2<>(o.f0, current.f1 + "-" + o.f1);
            }
        });

        keyedDataStream.print("foldWithRichFunction");

        try {
            env.execute("fold");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
