package com.pgman.goku.datastream.transformation;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.pojo.Order;
import com.pgman.goku.util.DateUtils;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 *
 * Flink时间语义
 *      事件时间(Event Time)
 *      摄入时间(Ingestion time)
 *      处理时间(Processing Time) ： 默认语义
 *
 * Flink时间语义编程第一步需要设置时间语义，例如：
 *  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
 *
 * Flink基于event time语义编程，处理迟到数据通过：
 *  1 watermark ： Flink中测量事件时间进程的机制是水位线。水位线作为数据流的一部分流动并带有时间戳t。对于无序流，水位线是至关重要的，属于延迟窗口触发机制的核心。
 *
 *      watermark = 进入窗口的最大时间戳 - 指定的延迟时间
 *      如果窗口的停止时间小于等于watermark，窗口被触发。
 *      当有新数据进入，watermark不变，窗口也会再次被触发。
 *      如果有 Watermark 同时也有 Allowed Lateness。那么窗口函数再次触发的条件 是：watermark < end-of-window + allowedLateness
 *
 *      多并行度watermark计算，取并行度最小的watermark作为算子的watermark
 *
 *  2 允许延迟：
 *
 *
 * 有序提取时间戳
 *  AscendingTimestampExtractor 指定
 *
 * 乱序提取时间戳
 *    周期性
 *    间断性
 *
 *
 */
public class EventTimeExample {

    public static void main(String[] args) {

        String groupId = "event-time-order";

//        eventTimeOrder(groupId);

//        eventTimeUnorder(groupId);

//        eventTimeUnorderAndAllowedLateness(groupId);

        eventTimeUnorderPunctuated(groupId);
    }



    /**
     * 间断性生成水位线
     *
     * @param groupId
     */
    public static void eventTimeUnorderPunctuated(String groupId){

        // 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        // 2 分配时间戳，从记录中提取时间戳信息
        SingleOutputStreamOperator<JSONObject> reduce = jsonText.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String text) throws Exception {
                return JSONUtils.getJSONObjectFromString(text);
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<JSONObject>() {

            Long maxEventTime = 0L;

            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(JSONObject lastElement, long extractedTimestamp) {
                if (lastElement.getInteger("order_status") == 4) {
                    if (maxEventTime < DateUtils.getTimestamp(lastElement.getString("create_time"), "yyyy-MM-dd HH:mm:ss")) {
                        maxEventTime = DateUtils.getTimestamp(lastElement.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
                    }
                    return new Watermark(maxEventTime - 3000L);
                }
                return null;
            }

            @Override
            public long extractTimestamp(JSONObject element, long previousElementTimestamp) {
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }

        }).keyBy(
                json -> json.getInteger("customer_id")
        ).timeWindow(
                Time.seconds(2)
        ).reduce(new ReduceFunction<JSONObject>() {
            @Override
            public JSONObject reduce(JSONObject t1, JSONObject t2) throws Exception {
                Integer orderAmt = t1.getInteger("order_amt") + t2.getInteger("order_amt");
                t2.put("order_amt", orderAmt);
                return t2;
            }
        });

        reduce.print("Punctuated");

        try {
            env.execute("event time");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 为有序数据源 指定时间戳
     *
     * @param groupId
     */
    public static void eventTimeOrder(String groupId){

        // 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        // 2 分配时间戳，从记录中提取时间戳信息
        SingleOutputStreamOperator<JSONObject> reduceDataStream = jsonText.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String text) throws Exception {
                return JSONUtils.getJSONObjectFromString(text);
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<JSONObject>() {
            @Override
            public long extractAscendingTimestamp(JSONObject element) {
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }
        }).keyBy(
                json -> json.getString("customer_id")
        ).timeWindow(Time.seconds(5)).reduce(new ReduceFunction<JSONObject>() {
            @Override
            public JSONObject reduce(JSONObject t1, JSONObject t2) throws Exception {
                Integer orderAmt = t1.getInteger("order_amt") + t2.getInteger("order_amt");
                t2.put("order_amt", orderAmt);
                return t2;
            }
        });

        reduceDataStream.print("event time");

        try {
            env.execute("event time");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 指定乱序时间戳
     * 周期性生成watermark
     * 间断性生成watermark
     *
     * @param groupId
     */
    public static void eventTimeUnorder(String groupId){

        // 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置周期间隔，表示每隔多长时间 计算一次 ，默认100ms
        env.getConfig().setAutoWatermarkInterval(200L);

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


        // 2 分配时间戳，从记录中提取时间戳信息
        // 2.1 通過 BoundedOutOfOrdernessTimestampExtractor 指定提取时间戳字段 和 水位线 ，即继承 flink 实现的方法
        SingleOutputStreamOperator<JSONObject> mapDataStream1 = jsonText.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String text) throws Exception {
                return JSONUtils.getJSONObjectFromString(text);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.seconds(3)) { // 参数 延迟的最大时间
            @Override
            public long extractTimestamp(JSONObject element) { // 提取时间戳 ，通过提取到的时间戳 和 延迟最大时间间隔 可以计算出 watermark
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }
        });


        // 2.2 通过 实现 AssignerWithPeriodicWatermarks 的自定义实现类 指定时间戳 和 水位线
        SingleOutputStreamOperator<JSONObject> mapDataStream2 = jsonText.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String text) throws Exception {
                return JSONUtils.getJSONObjectFromString(text);
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<JSONObject>() {

            Long maxEventTime = 0L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(maxEventTime - 3000L); // watermark = maxEventTime - 延迟时间
            }

            @Override
            public long extractTimestamp(JSONObject element, long previousElementTimestamp) { // 提取时间戳

                // 计算最大时间戳
                if (maxEventTime < DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss")) {
                    maxEventTime = DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
                }
                // 返回当前时间戳
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }

        });


        // 定义后续操作
        SingleOutputStreamOperator<JSONObject> reduceDataStream1 = mapDataStream1.keyBy(
                json -> json.getString("customer_id")
        ).timeWindow(Time.seconds(5)).reduce(new ReduceFunction<JSONObject>() {
            @Override
            public JSONObject reduce(JSONObject t1, JSONObject t2) throws Exception {
                Integer orderAmt = t1.getInteger("order_amt") + t2.getInteger("order_amt");
                t2.put("order_amt", orderAmt);
                return t2;
            }
        });


        SingleOutputStreamOperator<JSONObject> reduceDataStream2 = mapDataStream2.keyBy(
                json -> json.getString("customer_id")
        ).timeWindow(Time.seconds(5)).reduce(new ReduceFunction<JSONObject>() {
            @Override
            public JSONObject reduce(JSONObject t1, JSONObject t2) throws Exception {
                Integer orderAmt = t1.getInteger("order_amt") + t2.getInteger("order_amt");
                t2.put("order_amt", orderAmt);
                return t2;
            }
        });

        reduceDataStream1.print("Flink内置");

        reduceDataStream2.print("自定义");

        try {
            env.execute("Extract Timestamps");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 指定乱序时间戳,同时允许延迟数据
     *
     * 周期性生成watermark
     *
     * 时间概念：
     * 1 watermark周期间隔：定义每隔多久计算一次waterMark，默认100ms
     *      env.getConfig().setAutoWatermarkInterval(200L);
     * 2 watermark最大允许延迟时间：用于计算watermark
     * 3 最大允许延时间隔：用于定义超过watermark多久还可以触发窗口计算，丢失数据可以进入测输出流进行处理
     *
     * @param groupId
     */
    public static void eventTimeUnorderAndAllowedLateness(String groupId){

        // 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置周期间隔，表示每隔多长时间 计算一次 ，默认100ms
        env.getConfig().setAutoWatermarkInterval(200L);

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


        // 2 分配时间戳，从记录中提取时间戳信息
        // 2.1 通過 BoundedOutOfOrdernessTimestampExtractor 指定提取时间戳字段 和 水位线 ，即继承 flink 实现的方法
        SingleOutputStreamOperator<Order> mapDataStream = jsonText.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                Order order = new Order();
                order.setId(json.getInteger("id"));
                order.setCustomerId(json.getInteger("customer_id"));
                order.setOrderStatus(json.getInteger("order_status"));
                order.setOrderAmt(json.getInteger("order_amt"));
                order.setCreateTime(json.getString("create_time"));

                return order;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(1)) { // 参数 延迟的最大时间
            @Override
            public long extractTimestamp(Order element) { // 提取时间戳 ，通过提取到的时间戳 和 延迟最大时间间隔 可以计算出 watermark
                return DateUtils.getTimestamp(element.getCreateTime(), "yyyy-MM-dd HH:mm:ss");
            }
        });

        OutputTag<Order> late = new OutputTag<Order>("late"){}; // 定义测输出流标识

        // 定义后续操作
        SingleOutputStreamOperator<Order> reduceDataStream =
                mapDataStream
                .keyBy("customerId")
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(3))
                .sideOutputLateData(late) // 延迟超过3s数据 进入 测输出流
                .reduce(new ReduceFunction<Order>() {
                    @Override
                    public Order reduce(Order t1, Order t2) throws Exception {
                        Integer orderAmt = t1.getOrderAmt() + t2.getOrderAmt();
                        t2.setOrderAmt(orderAmt);
                        return t2;
                    }
                });

        reduceDataStream.print("allowedLateness");

        reduceDataStream.getSideOutput(late).print("late"); // 处理迟到数据


        try {
            env.execute("allowedLateness");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
