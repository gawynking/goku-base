package com.pgman.goku.datastream.transformation;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.DateUtils;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Collection;
import java.util.Properties;

/**
 * side output用于处理超出 允许延迟的最大时间的数据，目的防止数据丢失
 *
 * 在side output处理之前，首先通过 watermark 延迟时间水位线触发机制处理 短时间内延迟数据
 * 其次 通过 allowedLateness(Time.seconds(x)) 定义超过水位线后延续延迟触发的最大时间间隔
 * 如果步骤2还没有处理的数据，将流入测输出流进行处理，从而保证最终数据的完整性，一致性
 *
 */
public class SideOutputExample {

    public static void main(String[] args) {

        String groupId = "side-order";

//        sideOutputWithLateness(groupId);

        sideOutputWithProcess(groupId);
    }


    /**
     * 测输出流 处理延时数据
     *
     * @param groupId
     */
    public static void sideOutputWithLateness(String groupId){

        // 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(200L);

        // 定义kafka参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConfigurationManager.getString("broker.list"));
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("late") {
        };

        // 注册kafka数据源
        DataStreamSource<String> jsonText = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        SingleOutputStreamOperator<JSONObject> reduce = jsonText.map(new MapFunction<String, JSONObject>() {

            @Override
            public JSONObject map(String text) throws Exception {
                return JSONUtils.getJSONObjectFromString(text);

            }

        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(JSONObject element) {
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }
        }).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("customer_id");
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3)) // 允许延迟最多3s钟
                .sideOutputLateData(outputTag)
                .reduce(new ReduceFunction<JSONObject>() {
                    @Override
                    public JSONObject reduce(JSONObject t1, JSONObject t2) throws Exception {
                        Integer orderAmt = t1.getInteger("order_amt") + t2.getInteger("order_amt");
                        t2.put("order_amt", orderAmt);
                        return t2;
                    }
                });

        reduce.print("标准输出");
        reduce.getSideOutput(outputTag).print("延时输出");

        try {
            env.execute("SideOutput");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     *
     * 使用ProcessFunction手动生成侧流数据
     *
     * @param groupId
     */
    public static void sideOutputWithProcess(String groupId){

        // 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(200L);

        // 定义kafka参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConfigurationManager.getString("broker.list"));
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("late") {};

        // 注册kafka数据源
        DataStreamSource<String> jsonText = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<JSONObject> process = jsonText.map(new MapFunction<String, JSONObject>() {

            @Override
            public JSONObject map(String text) throws Exception {
                return JSONUtils.getJSONObjectFromString(text);

            }

        }).process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                if(value.getInteger("order_status") == 4){
                    out.collect(value);
                }else{
                    ctx.output(outputTag, value);
                }

            }
        });

        process.print("标准输出");

        process.getSideOutput(outputTag).print("侧流输出");


        try {
            env.execute("SideOutput");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
