package com.pgman.goku.datastream.transformation;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 两个或多个数据流的联合，创建包含来自所有流的所有元素的新流。注意：如果将数据流与其自身联合，则会在结果流中获取两次元素。
 *
 * 被联合的 DataStream 内置数据类型需要一致
 */
public class UnionExample {

    public static void main(String[] args) {

        String groupId = "union-order";

        union(groupId);

    }

    /**
     *
     * @param groupId
     */
    public static void union(String groupId){

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

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDataStream = jsonText.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple2<>(json.getString("customer_id"), json.getInteger("order_amt"));
            }
        }).keyBy(0);

        // sum
        SingleOutputStreamOperator<Tuple4<String, Integer,Integer,Integer>> sumDataStream =
                keyedDataStream.sum(1).map(new MapFunction<Tuple2<String, Integer>, Tuple4<String, Integer,Integer,Integer>>() {
                    @Override
                    public Tuple4<String, Integer,Integer,Integer> map(Tuple2<String, Integer> tuple2) throws Exception {
                        return new Tuple4<>(tuple2.f0,tuple2.f1,0,0);
                    }
                });

        // min
        SingleOutputStreamOperator<Tuple4<String, Integer,Integer,Integer>> minDataStream =
                keyedDataStream.min(1).map(new MapFunction<Tuple2<String, Integer>, Tuple4<String, Integer,Integer,Integer>>() {
                    @Override
                    public Tuple4<String, Integer,Integer,Integer> map(Tuple2<String, Integer> tuple2) throws Exception {
                        return new Tuple4<>(tuple2.f0,0,tuple2.f1,0);
                    }
                });

        // max
        SingleOutputStreamOperator<Tuple4<String, Integer,Integer,Integer>> maxDataStream =
                keyedDataStream.max(1).map(new MapFunction<Tuple2<String, Integer>, Tuple4<String, Integer,Integer,Integer>>() {
                    @Override
                    public Tuple4<String, Integer,Integer,Integer> map(Tuple2<String, Integer> tuple2) throws Exception {
                        return new Tuple4<>(tuple2.f0,0,0,tuple2.f1);
                    }
                });


        // union 算子
        DataStream<Tuple4<String, Integer,Integer,Integer>> unionDataStream = sumDataStream.union(minDataStream, maxDataStream);

        unionDataStream.print("union");

        try {
            env.execute("fold");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
