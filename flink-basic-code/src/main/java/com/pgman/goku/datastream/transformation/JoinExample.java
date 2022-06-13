package com.pgman.goku.datastream.transformation;


import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.DateUtils;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Flink的join操作
 *
 * 流计算关联场景：
 *  流流join
 *      窗口join
 *          滚动窗口join
 *          滑动窗口join
 *          session窗口join
 *      区间join
 *
 *  流批join ： 个人觉得数据仓库抽象的重点，流流join数据不一致风险系数比较高且基于数仓复杂的业务场景而言，流流join应该在kappa数仓架构下尽量避免使用。
 *      优化：可以使用google Cache技术进行优化
 *
 *
 * 窗口join一般用法：
 * stream.join(otherStream)
 *      .where(<KeySelector>)
 *      .equalTo(<KeySelector>)
 *      .window(<WindowAssigner>)
 *      .apply(<JoinFunction>)
 *
 *
 */
public class JoinExample {

    public static void main(String[] args) {

        String groupId = "join-order";

        streamJoinStreamOfWindowJoinWithTumbleWindow(groupId);

        streamJoinStreamOfWindowJoinWithSlideWindow(groupId);

        streamJoinStreamOfWindowJoinWithSessionWindow(groupId);

        streamJoinStreamOfIntervalJoin(groupId);


    }


    /**
     * 滚动窗口join
     *
     * @param groupId
     */
    public static void streamJoinStreamOfWindowJoinWithTumbleWindow(String groupId){

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
        DataStreamSource<String> order = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        DataStreamSource<String> orderDetail = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.detail.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<JSONObject> mapOrder = order.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONUtils.getJSONObjectFromString(s);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(JSONObject element) {
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }
        });

        SingleOutputStreamOperator<JSONObject> mapOrderDetail = orderDetail.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONUtils.getJSONObjectFromString(s);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(JSONObject element) {
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }
        });


        DataStream<Tuple2<JSONObject,JSONObject>> apply1 = mapOrder
                .join(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() { // 指定左表连接key
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() { // 指定右表连接key
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<JSONObject, JSONObject, Tuple2<JSONObject,JSONObject>>() {
                    @Override
                    public Tuple2 join(JSONObject jsonObject1, JSONObject jsonObject2) throws Exception {
                        return new Tuple2(jsonObject1, jsonObject2);
                    }
                });

        DataStream<Tuple2<JSONObject, JSONObject>> apply2 = mapOrder
                .join(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new RichJoinFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    String tag = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        tag = "Rich";
                    }

                    @Override
                    public Tuple2<JSONObject, JSONObject> join(JSONObject jsonObject1, JSONObject jsonObject2) throws Exception {
                        jsonObject1.put("tag", tag);
                        jsonObject2.put("tag", tag);
                        return new Tuple2<>(jsonObject1, jsonObject2);
                    }
                });

        DataStream<Tuple2<JSONObject, JSONObject>> apply3 = mapOrder
                .join(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new FlatJoinFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    @Override
                    public void join(JSONObject jsonObject1, JSONObject jsonObject2, Collector<Tuple2<JSONObject, JSONObject>> collector) throws Exception {
                        collector.collect(new Tuple2<>(jsonObject1, jsonObject2));
                    }
                });

        DataStream<Tuple2<JSONObject, JSONObject>> apply4 = mapOrder
                .join(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new RichFlatJoinFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    String tag = null;

                    @Override
                    public void join(JSONObject jsonObject1, JSONObject jsonObject2, Collector<Tuple2<JSONObject, JSONObject>> collector) throws Exception {
                        jsonObject1.put("tag", tag);
                        jsonObject2.put("tag", tag);

                        collector.collect(new Tuple2<>(jsonObject1, jsonObject2));
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        tag = "Rich";
                    }
                });


        DataStream<Tuple2<JSONObject, JSONObject>> apply5 = mapOrder.coGroup(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    @Override
                    public void coGroup(Iterable<JSONObject> iterable1, Iterable<JSONObject> iterable2, Collector<Tuple2<JSONObject, JSONObject>> collector) throws Exception {

                        for (JSONObject jsonObject1 : iterable1) {
                            for (JSONObject jsonObject2 : iterable2) {
                                collector.collect(new Tuple2<>(jsonObject1, jsonObject2));
                            }
                        }

                    }
                });

        DataStream<Tuple2<JSONObject, JSONObject>> apply6 = mapOrder.coGroup(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new RichCoGroupFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    String tag = null;

                    @Override
                    public void setRuntimeContext(RuntimeContext t) {
                        super.setRuntimeContext(t);
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        tag = "Rich";
                    }

                    @Override
                    public void coGroup(Iterable<JSONObject> iterable1, Iterable<JSONObject> iterable2, Collector<Tuple2<JSONObject, JSONObject>> collector) throws Exception {

                        for (JSONObject jsonObject1 : iterable1) {
                            jsonObject1.put("tag", tag);
                            for (JSONObject jsonObject2 : iterable2) {
                                jsonObject2.put("tag", tag);
                                collector.collect(new Tuple2<>(jsonObject1, jsonObject2));
                            }
                        }
                    }
                });

        apply1.print("join-apply1");
        apply2.print("join-apply2");
        apply3.print("join-apply3");
        apply4.print("join-apply4");

        apply5.print("coGroup-apply5");
        apply6.print("coGroup-apply6");

        try {
            env.execute("join");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    /**
     * 滑动窗口join
     *
     * @param groupId
     */
    public static void streamJoinStreamOfWindowJoinWithSlideWindow(String groupId){

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
        DataStreamSource<String> order = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        DataStreamSource<String> orderDetail = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.detail.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<JSONObject> mapOrder = order.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONUtils.getJSONObjectFromString(s);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(JSONObject element) {
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }
        });

        SingleOutputStreamOperator<JSONObject> mapOrderDetail = orderDetail.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONUtils.getJSONObjectFromString(s);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(JSONObject element) {
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }
        });


        DataStream<Tuple2<JSONObject,JSONObject>> apply1 = mapOrder
                .join(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() { // 指定左表连接key
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() { // 指定右表连接key
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .apply(new JoinFunction<JSONObject, JSONObject, Tuple2<JSONObject,JSONObject>>() {
                    @Override
                    public Tuple2 join(JSONObject jsonObject1, JSONObject jsonObject2) throws Exception {
                        return new Tuple2(jsonObject1, jsonObject2);
                    }
                });

        DataStream<Tuple2<JSONObject, JSONObject>> apply2 = mapOrder
                .join(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .apply(new RichJoinFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    String tag = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        tag = "Rich";
                    }

                    @Override
                    public Tuple2<JSONObject, JSONObject> join(JSONObject jsonObject1, JSONObject jsonObject2) throws Exception {
                        jsonObject1.put("tag", tag);
                        jsonObject2.put("tag", tag);
                        return new Tuple2<>(jsonObject1, jsonObject2);
                    }
                });

        DataStream<Tuple2<JSONObject, JSONObject>> apply3 = mapOrder
                .join(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .apply(new FlatJoinFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    @Override
                    public void join(JSONObject jsonObject1, JSONObject jsonObject2, Collector<Tuple2<JSONObject, JSONObject>> collector) throws Exception {
                        collector.collect(new Tuple2<>(jsonObject1, jsonObject2));
                    }
                });

        DataStream<Tuple2<JSONObject, JSONObject>> apply4 = mapOrder
                .join(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .apply(new RichFlatJoinFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    String tag = null;

                    @Override
                    public void join(JSONObject jsonObject1, JSONObject jsonObject2, Collector<Tuple2<JSONObject, JSONObject>> collector) throws Exception {
                        jsonObject1.put("tag", tag);
                        jsonObject2.put("tag", tag);

                        collector.collect(new Tuple2<>(jsonObject1, jsonObject2));
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        tag = "Rich";
                    }
                });


        DataStream<Tuple2<JSONObject, JSONObject>> apply5 = mapOrder.coGroup(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .apply(new CoGroupFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    @Override
                    public void coGroup(Iterable<JSONObject> iterable1, Iterable<JSONObject> iterable2, Collector<Tuple2<JSONObject, JSONObject>> collector) throws Exception {

                        for (JSONObject jsonObject1 : iterable1) {
                            for (JSONObject jsonObject2 : iterable2) {
                                collector.collect(new Tuple2<>(jsonObject1, jsonObject2));
                            }
                        }

                    }
                });

        DataStream<Tuple2<JSONObject, JSONObject>> apply6 = mapOrder.coGroup(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .apply(new RichCoGroupFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    String tag = null;

                    @Override
                    public void setRuntimeContext(RuntimeContext t) {
                        super.setRuntimeContext(t);
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        tag = "Rich";
                    }

                    @Override
                    public void coGroup(Iterable<JSONObject> iterable1, Iterable<JSONObject> iterable2, Collector<Tuple2<JSONObject, JSONObject>> collector) throws Exception {

                        for (JSONObject jsonObject1 : iterable1) {
                            jsonObject1.put("tag", tag);
                            for (JSONObject jsonObject2 : iterable2) {
                                jsonObject2.put("tag", tag);
                                collector.collect(new Tuple2<>(jsonObject1, jsonObject2));
                            }
                        }
                    }
                });



        apply1.print("join-apply1");
        apply2.print("join-apply2");
        apply3.print("join-apply3");
        apply4.print("join-apply4");

        apply5.print("coGroup-apply5");
        apply6.print("coGroup-apply6");

        try {
            env.execute("join");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 会话窗口join
     *
     * @param groupId
     */
    public static void streamJoinStreamOfWindowJoinWithSessionWindow(String groupId){

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
        DataStreamSource<String> order = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        DataStreamSource<String> orderDetail = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.detail.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<JSONObject> mapOrder = order.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONUtils.getJSONObjectFromString(s);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(JSONObject element) {
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }
        });

        SingleOutputStreamOperator<JSONObject> mapOrderDetail = orderDetail.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONUtils.getJSONObjectFromString(s);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(JSONObject element) {
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }
        });


        DataStream<Tuple2<JSONObject,JSONObject>> apply1 = mapOrder
                .join(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() { // 指定左表连接key
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() { // 指定右表连接key
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .apply(new JoinFunction<JSONObject, JSONObject, Tuple2<JSONObject,JSONObject>>() {
                    @Override
                    public Tuple2 join(JSONObject jsonObject1, JSONObject jsonObject2) throws Exception {
                        return new Tuple2(jsonObject1, jsonObject2);
                    }
                });

        DataStream<Tuple2<JSONObject, JSONObject>> apply2 = mapOrder
                .join(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .apply(new RichJoinFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    String tag = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        tag = "Rich";
                    }

                    @Override
                    public Tuple2<JSONObject, JSONObject> join(JSONObject jsonObject1, JSONObject jsonObject2) throws Exception {
                        jsonObject1.put("tag", tag);
                        jsonObject2.put("tag", tag);
                        return new Tuple2<>(jsonObject1, jsonObject2);
                    }
                });

        DataStream<Tuple2<JSONObject, JSONObject>> apply3 = mapOrder
                .join(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .apply(new FlatJoinFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    @Override
                    public void join(JSONObject jsonObject1, JSONObject jsonObject2, Collector<Tuple2<JSONObject, JSONObject>> collector) throws Exception {
                        collector.collect(new Tuple2<>(jsonObject1, jsonObject2));
                    }
                });

        DataStream<Tuple2<JSONObject, JSONObject>> apply4 = mapOrder
                .join(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .apply(new RichFlatJoinFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    String tag = null;

                    @Override
                    public void join(JSONObject jsonObject1, JSONObject jsonObject2, Collector<Tuple2<JSONObject, JSONObject>> collector) throws Exception {
                        jsonObject1.put("tag", tag);
                        jsonObject2.put("tag", tag);

                        collector.collect(new Tuple2<>(jsonObject1, jsonObject2));
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        tag = "Rich";
                    }
                });


        DataStream<Tuple2<JSONObject, JSONObject>> apply5 = mapOrder.coGroup(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .apply(new CoGroupFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    @Override
                    public void coGroup(Iterable<JSONObject> iterable1, Iterable<JSONObject> iterable2, Collector<Tuple2<JSONObject, JSONObject>> collector) throws Exception {

                        for (JSONObject jsonObject1 : iterable1) {
                            for (JSONObject jsonObject2 : iterable2) {
                                collector.collect(new Tuple2<>(jsonObject1, jsonObject2));
                            }
                        }

                    }
                });

        DataStream<Tuple2<JSONObject, JSONObject>> apply6 = mapOrder.coGroup(mapOrderDetail)
                .where(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("id");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Integer>() {
                    @Override
                    public Integer getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getInteger("order_id");
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .apply(new RichCoGroupFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    String tag = null;

                    @Override
                    public void setRuntimeContext(RuntimeContext t) {
                        super.setRuntimeContext(t);
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        tag = "Rich";
                    }

                    @Override
                    public void coGroup(Iterable<JSONObject> iterable1, Iterable<JSONObject> iterable2, Collector<Tuple2<JSONObject, JSONObject>> collector) throws Exception {

                        for (JSONObject jsonObject1 : iterable1) {
                            jsonObject1.put("tag", tag);
                            for (JSONObject jsonObject2 : iterable2) {
                                jsonObject2.put("tag", tag);
                                collector.collect(new Tuple2<>(jsonObject1, jsonObject2));
                            }
                        }
                    }
                });


        apply1.print("join-apply1");
        apply2.print("join-apply2");
        apply3.print("join-apply3");
        apply4.print("join-apply4");

        apply5.print("coGroup-apply5");
        apply6.print("coGroup-apply6");

        try {
            env.execute("join");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 区间join
     *
     * 区间join仅支持 eventtime 语义
     *
     * @param groupId
     */
    public static void streamJoinStreamOfIntervalJoin(String groupId){

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
        DataStreamSource<String> order = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        DataStreamSource<String> orderDetail = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.detail.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        KeyedStream<JSONObject, Integer> keyedOrder = order.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONUtils.getJSONObjectFromString(s);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(JSONObject element) {
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }
        }).keyBy(new KeySelector<JSONObject, Integer>() {
            @Override
            public Integer getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getInteger("id");
            }
        });

        KeyedStream<JSONObject, Integer> keyedOrderDetail = orderDetail.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONUtils.getJSONObjectFromString(s);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(JSONObject element) {
                return DateUtils.getTimestamp(element.getString("create_time"), "yyyy-MM-dd HH:mm:ss");
            }
        }).keyBy(new KeySelector<JSONObject, Integer>() {
            @Override
            public Integer getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getInteger("order_id");
            }
        });


        SingleOutputStreamOperator<Tuple2<JSONObject, JSONObject>> process1 = keyedOrder.intervalJoin(keyedOrderDetail)
                .between(Time.seconds(-3), Time.seconds(3))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, Context ctx, Collector<Tuple2<JSONObject, JSONObject>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        SingleOutputStreamOperator<Tuple2<JSONObject, JSONObject>> process2 = keyedOrder.intervalJoin(keyedOrderDetail)
                .between(Time.seconds(-3), Time.seconds(3))
                .lowerBoundExclusive()
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<JSONObject, JSONObject, Tuple2<JSONObject, JSONObject>>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, Context ctx, Collector<Tuple2<JSONObject, JSONObject>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        process1.print("join-process1");
        process2.print("join-process2");


        try {
            env.execute("join");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
