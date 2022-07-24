package com.goku.datastream;

import com.alibaba.fastjson.JSONObject;
import com.goku.config.ConfigurationManager;
import com.pgman.goku.util.DateUtils;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SerializedValue;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 *
 * Windows可以在常规DataStream上定义。Windows根据某些特征（例如，在最后5秒内到达的数据）对所有流事件进行分组。
 * 警告：在许多情况下，这是非并行转换。所有记录将收集在windowAll操作符的一个任务中。
 * WindowAll是非并行的，这点在数据量较大的任务里需要注意，可以考虑使用常规window方法，通过hashKey方式提升并行度。
 *
 * Flink模拟微批次处理 需要 通过合适的时间窗口定义
 *
 * window 分为 time window 和 session window。time window 时间语义可以指定为 Event Time / Processing Time / Ingestion Time ，event time语义可能需要定义watermark跟踪时间进度。
 *
 *
 * 1.1 键控窗口定义：
 *  stream
 *        .keyBy(...)               <-  keyed versus non-keyed windows
 *        .window(...)              <-  required: "assigner"
 *        [.trigger(...)]            <-  optional: "trigger" (else default trigger)
 *        [.evictor(...)]            <-  optional: "evictor" (else no evictor)
 *        [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
 *        [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
 *        .reduce/aggregate/fold/apply()      <-  required: "function"
 *        [.getSideOutput(...)]      <-  optional: "output tag"
 *
 * 1.2 非键控窗口定义
 *  stream
 *        .windowAll(...)           <-  required: "assigner"
 *        [.trigger(...)]            <-  optional: "trigger" (else default trigger)
 *        [.evictor(...)]            <-  optional: "evictor" (else no evictor)
 *        [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
 *        [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
 *        .reduce/aggregate/fold/apply()      <-  required: "function"
 *        [.getSideOutput(...)]      <-  optional: "output tag"
 *
 *
 * 2 窗口生命周期 :
 *      当属于该窗口的第一个元素到达时，就会创建一个窗口，当时间（事件或处理时间）超过其结束时间戳加上用户指定的允许延迟（请参见允许延迟）时，该窗口将被完全删除。
 *      Flink保证只删除基于时间的窗口，而不删除其他类型的窗口，例如全局窗口
 *
 * 3 窗口分为 键控与非键控窗口 ，原则上来说，键控窗口可以用来替换非键控窗口，就像flatMap可以替换map算子一样
 *
 * 4 window分类：
 *  滚动窗口
 *  滑动窗口
 *  会话窗口
 *  全局窗口
 *
 * 5 窗口函数
 *  ReduceFunction ： 增量
 *  AggregateFunction ： 增量
 *  FoldFunction ： 增量
 *  ProcessWindowFunction ： 全量
 *      在ProcessWindowFunction中使用每个窗口的状态
 *          globalState()，允许访问没有作用于窗口的键控状态
 *          windowState()，允许访问也限定在窗口范围内的键控状态
 *
 * 6 延迟数据
 * Flink允许为窗口操作符指定允许的最大迟到时间。
 * 使用Flink的侧输出功能，您可以获得最近丢弃的数据流。
 *
 */
public class WindowExample {

    public static void main(String[] args) {

        String groupId = "window-order";

        windowOfTumbleAndProcessingTime(groupId);
        windowOfTumbleAndEventTime(groupId);

        windowOfSlideAndProcessingTime(groupId);
        windowOfSlideAndEventTime(groupId);

        windowOfSessionAndProcessingTime(groupId);
        windowOfSessionAndEventTime(groupId);

        windowOfReduceFunction(groupId);
        windowOfAggregateFunction(groupId);

        windowOfAggregates(groupId);

        windowOfFoldFunction(groupId);

        windowOfApply(groupId);

        windowOfProcess(groupId);

        windowIncrementalAggregationWithReduceFunctionAndProcessWindowFunction(groupId);

        windowIncrementalAggregationWithAggregateFunctionAndProcessWindowFunction(groupId);

        windowIncrementalAggregationWithFoldFunctionAndProcessWindowFunction(groupId);


    }


    /**
     *
     * 滚动窗口
     * 基于 ProcessingTime 的时间滚动窗口
     *
     * @param groupId
     *
     */
    public static void windowOfTumbleAndProcessingTime(String groupId){

        // 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 定义kafka参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConfigurationManager.getString("broker.list"));
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 注册kafka数据源
        DataStreamSource<String> jsonText = env.addSource(
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple2<>(json.getInteger("customer_id"), json.getInteger("order_amt"));

            }
        });

        // 1 timeWindow定义基于 ProcessingTime 的时间滚动窗口
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> keyedDataStream1 = mapDataStream.keyBy(0).timeWindow(Time.seconds(5)).sum(1);

        // 2 window定义基于 ProcessingTime 的时间滚动窗口
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> keyedDataStream2 = mapDataStream.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(1);

        keyedDataStream1.print("window1");
        keyedDataStream2.print("window2");

        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     *
     * 滚动窗口
     *
     * 基于 EventTime 的时间滚动窗口
     * @param groupId
     */
    public static void windowOfTumbleAndEventTime(String groupId){

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

                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )

        );


        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getInteger("customer_id"), json.getString("create_time"), json.getInteger("order_amt"));

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Integer>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3<Integer, String, Integer> element) {
                return DateUtils.getTimestamp(element.f1, "yyyy-MM-dd HH:mm:ss");
            }

        });


        // 1 timeWindow定义基于 EventTime 的时间滚动窗口
        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> keyedDataStream1 = mapDataStream.keyBy(0).timeWindow(Time.seconds(5)).sum(2);

        // 2 window定义基于 EventTime 的时间滚动窗口
        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> keyedDataStream2 = mapDataStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5))).sum(2);

        keyedDataStream1.print("window1");
        keyedDataStream2.print("window2");

        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    /**
     *
     * 滑动窗口
     * 基于 ProcessingTime 的时间滑动窗口
     *
     * @param groupId
     *
     */
    public static void windowOfSlideAndProcessingTime(String groupId){

        // 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 定义kafka参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConfigurationManager.getString("broker.list"));
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 注册kafka数据源
        DataStreamSource<String> jsonText = env.addSource(
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple2<>(json.getInteger("customer_id"), json.getInteger("order_amt"));

            }
        });

        // 1 timeWindow定义基于 ProcessingTime 的时间滑动窗口
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> keyedDataStream1 = mapDataStream.keyBy(0).timeWindow(Time.seconds(10),Time.seconds(5)).sum(1);

        // 2 window定义基于 ProcessingTime 的时间滑动窗口
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> keyedDataStream2 = mapDataStream.keyBy(0).window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5))).sum(1);

        keyedDataStream1.print("window1");
        keyedDataStream2.print("window2");

        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     *
     * 滑动窗口
     *
     * 基于 EventTime 的时间滑动窗口
     * @param groupId
     */
    public static void windowOfSlideAndEventTime(String groupId){

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
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getInteger("customer_id"), json.getString("create_time"), json.getInteger("order_amt"));

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Integer>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3<Integer, String, Integer> element) {
                return DateUtils.getTimestamp(element.f1, "yyyy-MM-dd HH:mm:ss");
            }

        });


        // 1 timeWindow定义基于 EventTime 的时间滚动窗口
        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> keyedDataStream1 = mapDataStream.keyBy(0).timeWindow(Time.seconds(10),Time.seconds(5)).sum(2);

        // 2 window定义基于 EventTime 的时间滚动窗口
        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> keyedDataStream2 = mapDataStream.keyBy(0).window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5))).sum(2);

        keyedDataStream1.print("window1");
        keyedDataStream2.print("window2");

        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     *
     * 会话窗口
     * 基于 ProcessingTime 的时间会话窗口
     *
     * @param groupId
     *
     */
    public static void windowOfSessionAndProcessingTime(String groupId){

        // 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 定义kafka参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConfigurationManager.getString("broker.list"));
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 注册kafka数据源
        DataStreamSource<String> jsonText = env.addSource(
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple2<>(json.getInteger("customer_id"), json.getInteger("order_amt"));

            }
        });

        // 1 静态session窗口
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> keyedDataStream1 = mapDataStream.keyBy(0).window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).sum(1);

        // 2 动态session窗口
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> keyedDataStream2 = mapDataStream.keyBy(0).window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<Integer, Integer>>() {

            @Override
            public long extract(Tuple2<Integer, Integer> element) {
                // determine and return session gap : 此处定义返回的session时长
                return element.f1 % 15 + 1;
            }

        })).sum(1);

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> keyedDataStream3 = mapDataStream.keyBy(0).window(DynamicProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<Integer, Integer>>() {
            @Override
            public long extract(Tuple2<Integer, Integer> element) {
                return element.f1 % 15 + 1;
            }
        })).sum(1);


        keyedDataStream1.print("window1");
        keyedDataStream2.print("window2");
        keyedDataStream3.print("window3");

        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     *
     * 会话窗口
     *
     * 基于 EventTime 的时间会话窗口
     * @param groupId
     */
    public static void windowOfSessionAndEventTime(String groupId){

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
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getInteger("customer_id"), json.getString("create_time"), json.getInteger("order_amt"));

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Integer>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3<Integer, String, Integer> element) {
                return DateUtils.getTimestamp(element.f1, "yyyy-MM-dd HH:mm:ss");
            }

        });


        // 1 静态session会话
        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> keyedDataStream1 = mapDataStream.keyBy(0).window(EventTimeSessionWindows.withGap(Time.seconds(10))).sum(2);

        // 2 动态session会话
        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> keyedDataStream2 = mapDataStream.keyBy(0).window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple3<Integer, String, Integer>>() {
            @Override
            public long extract(Tuple3<Integer, String, Integer> element) {
                return element.f2 % 15 + 1;
            }
        })).sum(2);

        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> keyedDataStream3 = mapDataStream.keyBy(0).window(DynamicEventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple3<Integer, String, Integer>>() {
            @Override
            public long extract(Tuple3<Integer, String, Integer> element) {
                return element.f2 % 15 + 1;
            }
        })).sum(2);


        keyedDataStream1.print("window1");
        keyedDataStream2.print("window2");
        keyedDataStream3.print("window3");

        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 窗口函数之 ReduceFunction
     *  ReduceFunction指定如何组合输入中的两个元素以生成相同类型的输出元素。Flink使用ReduceFunction以增量方式聚合窗口的元素。
     *
     * 窗口函数 reduce不支持richFunction
     *
     * @param groupId
     */
    public static void windowOfReduceFunction(String groupId){

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
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getInteger("customer_id"), json.getString("create_time"), json.getInteger("order_amt"));

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Integer>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3<Integer, String, Integer> element) {
                return DateUtils.getTimestamp(element.f1, "yyyy-MM-dd HH:mm:ss");
            }

        });

        // 应用窗口函数
        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> reduce1 =
                mapDataStream
                        .keyBy(0)
                        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                        .reduce(new ReduceFunction<Tuple3<Integer, String, Integer>>() {
                            @Override
                            public Tuple3<Integer, String, Integer> reduce(Tuple3<Integer, String, Integer> t1, Tuple3<Integer, String, Integer> t2) throws Exception {
                                return new Tuple3<>(t2.f0, t2.f1, t1.f2 + t2.f2);
                            }
                        });

        reduce1.print("reduceFunction");

        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 窗口函数之 AggregateFunction
     * AggregateFunction 是ReduceFunction的广义版本，它有三种类型:输入类型(IN)、累加器类型(ACC)和输出类型(OUT)。输入类型是输入流中的元素类型，
     * AggregateFunction有一个方法可以将一个输入元素添加到一个累加器中。该接口还具有创建初始累加器、将两个累加器合并到一个累加器以及从累加器中提取输出(类型为OUT)的方法
     *
     * 与ReduceFunction相同，Flink将在输入元素到达时递增地聚合它们。
     *
     * 该算子不支持 富函数
     *
     * @param groupId
     */
    public static void windowOfAggregateFunction(String groupId){

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
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getInteger("customer_id"), json.getString("create_time"), json.getInteger("order_amt"));

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Integer>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3<Integer, String, Integer> element) {
                return DateUtils.getTimestamp(element.f1, "yyyy-MM-dd HH:mm:ss");
            }

        });

        // 应用窗口函数
        SingleOutputStreamOperator<Tuple2<Integer, Double>> aggregate1 = mapDataStream
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple3<Integer, String, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Double>>() {

                    Integer customerId = null;

                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return new Tuple2<>(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(Tuple3<Integer, String, Integer> element, Tuple2<Integer, Integer> accumulator) {
                        customerId = element.f0;
                        return new Tuple2<>(accumulator.f0 + element.f2, accumulator.f1 + 1);
                    }

                    @Override
                    public Tuple2<Integer, Double> getResult(Tuple2<Integer, Integer> accumulator) {
                        return new Tuple2<>(customerId, Double.valueOf(accumulator.f0 / accumulator.f1));
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> acc1, Tuple2<Integer, Integer> acc2) {
                        return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
                    }

                });

        aggregate1.print("aggregateFunction");

        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 应用在窗口上的简易聚合函数
     *
     * 聚合窗口的内容。min和minBy之间的差异是min返回最小值，而minBy返回该字段中具有最小值的元素（max和maxBy相同）。
     *
     * @param groupId
     */
    public static void windowOfAggregates(String groupId){

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
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getInteger("customer_id"), json.getString("create_time"), json.getInteger("order_amt"));

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Integer>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3<Integer, String, Integer> element) {
                return DateUtils.getTimestamp(element.f1, "yyyy-MM-dd HH:mm:ss");
            }

        });

        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> sum = mapDataStream
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(2);

        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> min = mapDataStream
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .min(2);

        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> max = mapDataStream
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .max(2);

        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> minBy = mapDataStream
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .minBy(2);

        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> maxBy = mapDataStream
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .maxBy(2);


        sum.print("sum");
        min.print("min");
        minBy.print("minBy");
        max.print("max");
        maxBy.print("maxBy");

        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     *
     * 窗口函数之 FoldFunction
     * FoldFunction指定如何将窗口的输入元素与输出类型的元素组合在一起。对于添加到窗口的每个元素和当前输出值，将递增地调用FoldFunction。第一个元素与输出类型的预定义初始值相结合。
     *
     * 将FoldFunction应用于窗口并返回折叠值。示例函数应用于序列（1,2,3,4,5）时，将序列折叠为字符串“start-1-2-3-4-5”
     *
     * 不支持富函数,fold()不能与会话窗口或其他可合并窗口一起使用。
     *
     * @param groupId
     */
    public static void windowOfFoldFunction(String groupId){

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
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getInteger("customer_id"), json.getString("create_time"), json.getInteger("order_amt"));

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Integer>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3<Integer, String, Integer> element) {
                return DateUtils.getTimestamp(element.f1, "yyyy-MM-dd HH:mm:ss");
            }

        });

//        SingleOutputStreamOperator<String> fold = mapDataStream
//                .keyBy(0)
//                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .fold("start", new FoldFunction<Tuple3<Integer, String, Integer>, String>() {
//                    @Override
//                    public String fold(String current, Tuple3<Integer, String, Integer> o) throws Exception {
//                        return current + "_" + o.f2;
//                    }
//                });
//
//        fold.print("fold");

        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 将一般函数应用于整个窗口。下面是一个手动求和窗口元素的函数。
     *
     * @param groupId
     */
    public static void windowOfApply(String groupId){

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
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getInteger("customer_id"), json.getString("create_time"), json.getInteger("order_amt"));

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Integer>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3<Integer, String, Integer> element) {
                return DateUtils.getTimestamp(element.f1, "yyyy-MM-dd HH:mm:ss");
            }

        });

        SingleOutputStreamOperator<String> apply = mapDataStream
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new WindowFunction<Tuple3<Integer, String, Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<Integer, String, Integer>> input, Collector<String> out) throws Exception {

                        Integer customerId = null;
                        Integer orderAmt = 0;
                        for (Tuple3 element : input) {
                            customerId = (Integer) element.f0;
                            orderAmt = orderAmt + (Integer) element.f2;

                        }

                        out.collect(customerId + ":" + orderAmt);
                    }
                });

        apply.print("apply");


        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 窗口函数 之 ProcessWindowFunction
     *
     * ProcessWindowFunction获取包含窗口所有元素的Iterable，以及可访问时间和状态信息的Context对象，这使其能够提供比其他窗口函数更多的灵活性。
     * 这是以性能和资源消耗为代价的，因为元素不能以递增方式聚合，而是需要在内部进行缓冲，直到认为窗口已准备好进行处理。
     *
     * process实现方法 ProcessWindowFunction 和 RichProcessWindowFunction 测试没有本质区别
     *
     * @param groupId
     */
    public static void windowOfProcess(String groupId){

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
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getInteger("customer_id"), json.getString("create_time"), json.getInteger("order_amt"));

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Integer>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3<Integer, String, Integer> element) {
                return DateUtils.getTimestamp(element.f1, "yyyy-MM-dd HH:mm:ss");
            }

        });

        SingleOutputStreamOperator<Object> process1 = mapDataStream
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple3<Integer, String, Integer>, Object, Tuple, TimeWindow>() {

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
                    public void clear(Context context) throws Exception {
                        super.clear(context);
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple3<Integer, String, Integer>> elements, Collector<Object> out) throws Exception {

                        String customerId = null;
                        Integer orderAmt = 0;

                        for (Tuple3 element : elements) {

                            customerId = element.f0.toString();
                            orderAmt = orderAmt + (Integer) element.f2;

                        }

                        out.collect(customerId + ":" + orderAmt);

                    }

                });

        SingleOutputStreamOperator<Object> process2 = mapDataStream
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new RichProcessWindowFunction<Tuple3<Integer, String, Integer>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple3<Integer, String, Integer>> elements, Collector<Object> out) throws Exception {

                        String customerId = null;
                        Integer orderAmt = 0;

                        for (Tuple3 element : elements) {

                            customerId = element.f0.toString();
                            orderAmt = orderAmt + (Integer) element.f2;

                        }

                        out.collect(customerId + ":" + orderAmt);

                    }
                });


        process1.print("process1");
        process2.print("process2");


        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * ProcessWindowFunction可以与ReduceFunction、AggregateFunction或FoldFunction组合，以便在元素到达窗口时增量地聚合它们。
     * 当窗口关闭时，ProcessWindowFunction将提供聚合结果。这允许它在访问ProcessWindowFunction的附加窗口元信息的同时递增地计算窗口。
     *
     * 使用ReduceFunction增量窗口聚合
     *
     * @param groupId
     */
    public static void windowIncrementalAggregationWithReduceFunctionAndProcessWindowFunction(String groupId){

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
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getInteger("customer_id"), json.getString("create_time"), json.getInteger("order_amt"));

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Integer>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3<Integer, String, Integer> element) {
                return DateUtils.getTimestamp(element.f1, "yyyy-MM-dd HH:mm:ss");
            }

        });


        // 实现窗口增量计算
        SingleOutputStreamOperator<Object> reduce = mapDataStream
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple3<Integer, String, Integer>>() { // 每接收到一条数据执行一次
                            @Override
                            public Tuple3<Integer, String, Integer> reduce(Tuple3<Integer, String, Integer> t1, Tuple3<Integer, String, Integer> t2) throws Exception {
                                return new Tuple3<>(t1.f0, "", t1.f2 > t2.f2 ? t1.f2 : t2.f2);
                            }
                        },
                        new ProcessWindowFunction<Tuple3<Integer, String, Integer>, Object, Tuple, TimeWindow>() {
                            @Override
                            public void process(Tuple tuple, Context context, Iterable<Tuple3<Integer, String, Integer>> elements, Collector<Object> out) throws Exception {
                                Integer max = elements.iterator().next().f2;
                                out.collect(new Tuple2<Long, Integer>(context.window().getStart(), max));
                            }
                        });

        reduce.print("reduce");


        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    /**
     * ProcessWindowFunction可以与ReduceFunction、AggregateFunction或FoldFunction组合，以便在元素到达窗口时增量地聚合它们。
     * 当窗口关闭时，ProcessWindowFunction将提供聚合结果。这允许它在访问ProcessWindowFunction的附加窗口元信息的同时递增地计算窗口。
     *
     * 使用 AggregateFunction 增量窗口聚合
     *
     * @param groupId
     */
    public static void windowIncrementalAggregationWithAggregateFunctionAndProcessWindowFunction(String groupId){

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
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getInteger("customer_id"), json.getString("create_time"), json.getInteger("order_amt"));

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Integer>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3<Integer, String, Integer> element) {
                return DateUtils.getTimestamp(element.f1, "yyyy-MM-dd HH:mm:ss");
            }

        });


        // 实现窗口增量计算
        SingleOutputStreamOperator<Object> aggregate = mapDataStream
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple3<Integer, String, Integer>, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return new Tuple2<>(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(Tuple3<Integer, String, Integer> element, Tuple2<Integer, Integer> acc) {
                        return new Tuple2<>(acc.f0 + element.f2, acc.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Integer> acc) {
                        return (double) (acc.f0 / acc.f1);
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> acc1, Tuple2<Integer, Integer> acc2) {
                        return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
                    }

                }, new ProcessWindowFunction<Double, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Double> elements, Collector<Object> out) throws Exception {
                        Double average = elements.iterator().next();
                        out.collect(new Tuple2<>(tuple.getField(0), average));
                    }
                });

        aggregate.print("aggregate");

        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * ProcessWindowFunction可以与ReduceFunction、AggregateFunction或FoldFunction组合，以便在元素到达窗口时增量地聚合它们。
     * 当窗口关闭时，ProcessWindowFunction将提供聚合结果。这允许它在访问ProcessWindowFunction的附加窗口元信息的同时递增地计算窗口。
     *
     * 使用 FoldFunction 增量窗口聚合
     *
     * @param groupId
     */
    public static void windowIncrementalAggregationWithFoldFunctionAndProcessWindowFunction(String groupId){

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
                new FlinkKafkaConsumer<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );


        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapDataStream = jsonText.map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return new Tuple3<>(json.getInteger("customer_id"), json.getString("create_time"), json.getInteger("order_amt"));

            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Integer>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3<Integer, String, Integer> element) {
                return DateUtils.getTimestamp(element.f1, "yyyy-MM-dd HH:mm:ss");
            }

        });


        // 实现窗口增量计算
//        SingleOutputStreamOperator<Object> fold = mapDataStream
//                .keyBy(0)
//                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .fold("start", new FoldFunction<Tuple3<Integer, String, Integer>, String>() {
//                    @Override
//                    public String fold(String current, Tuple3<Integer, String, Integer> o) throws Exception {
//                        return current + "_" + o.f2;
//                    }
//                }, new ProcessWindowFunction<String, Object, Tuple, TimeWindow>() {
//                    @Override
//                    public void process(Tuple tuple, Context context, Iterable<String> elements, Collector<Object> out) throws Exception {
//                        String fold = elements.iterator().next();
//                        out.collect(new Tuple3<>(tuple.getField(0), context.window().getEnd(), fold));
//                    }
//                });

//        fold.print("fold");

        try {
            env.execute("window");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



}
