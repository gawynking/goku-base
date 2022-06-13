package com.pgman.goku.datastream.transformation;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.DateUtils;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 *
 * 低级别process API示例,可以直接应用在keyBy分流后的KeyedStream之上，丰富了计算属性：stream.keyBy(...).process(new MyProcessFunction())
 *
 * process API 是flink中功能最强大的函数 ProcessFunction是一个低级流处理操作，可以访问所有（非循环）流应用程序的基本构建块：
 *      1 事件（流元素）
 *      2 状态（容错，一致，仅在键控流上）
 *      3 定时器（事件时间和处理时间，仅限键控流）
 *
 * 过程函数(ProcessFunction) 可以被认为一种提供了对有键状态(keyed state)和定时器(timers)访问的 FlatMapFunction。每在输入流中收到一个事件，过程函数就会被触发来对事件进行处理。
 *
 * 定时器则允许程序对处理时间和事件时间(event time)的改变做出反应。每次对 processElement(...) 的调用都能拿到一个上下文(Context)对象,这个对象能访问到所处理元素事件时间的时间戳,
 * 还有 定时服务器(TimerService) 。定时服务器(TimerService)可以为尚未发生的处理时间或事件时间实例注册回调函数。当一个定时器到达特定的时间实例时，
 * onTimer(...)方法就会被调用。在这个函数的调用期间，所有的状态(states)都会再次对应定时器被创建时key所属的states，同时被触发的回调函数也能操作这些状态。
 *
 *
 * 低层级关联(Low-level Joins)：维度延迟场景
 *      为了在两个输入源实现低层次的操作，应用可以使用 CoProcessFunction。该函数绑定了连个不同的输入源并且会对从两个输入源中得到的记录分别调用 processElement1(...)
 *      和 processElement2(...) 方法。
 *      可以按下面的步骤来实现一个低层典型的连接操作：
 *      为一个(或两个)输入源创建一个状态(state)对象
 *      在从输入源收到元素时更新这个状态(state)对象
 *      在从另一个输入源接收到元素时，扫描这个state对象并产出连接的结果
 *      比如，你正在把顾客数据和交易数据做一个连接，并且为顾客数据保存了状态(state)。如果你担心因为事件乱序导致不能得到完整和准确的连接结果，你可以用定时器来 控制，
 *      当顾客数据的水位线(watermark)时间超过了那笔交易的时间时，再进行计算和产出连接的结果。
 *
 *
 */
public class ProcessExample {

    public static void main(String[] args) {

        String groupId = "process-order";

//        processOnKeyed(groupId);

        processOnNonKeyed(groupId);

    }


    /**
     * 在KeyedStream上应用 process 底层算子实现基于状态的编程
     *
     * @param groupId
     */
    public static void processOnKeyed(String groupId){

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

        SingleOutputStreamOperator<JSONObject> map = jsonText.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String text) throws Exception {
                return JSONUtils.getJSONObjectFromString(text);
            }
        });

        /**
         * 在keyedStream上应用process算子
         */
        SingleOutputStreamOperator<String> process = map.keyBy(jsonObject -> jsonObject.getInteger("customer_id")).process(new KeyedProcessFunction<Integer, JSONObject, String>() {

            // 注册value对象
            private ValueState<Long> timeState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeState", Long.class));
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);

                String warnString = "触发时间 ：" + timestamp + " , 客户ID ： " + ctx.getCurrentKey();
//                System.out.println(warnString);
                out.collect(warnString);
                timeState.clear();
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

//                Long nowTime = ctx.timerService().currentProcessingTime();
                Long valueTimestamp = DateUtils.getTimestamp(value.getString("create_time"), "yyyy-MM-dd HH:mm:ss");

                Long time = timeState.value();
//                System.out.println("time 值 ： " + time + " 差值 ：" + (valueTimestamp - ((time == null)?0:time)));

                if (time == null) {
                    timeState.update(valueTimestamp);
                    ctx.timerService().registerProcessingTimeTimer(valueTimestamp + 30000L);
                }

                if ((time != null) && valueTimestamp > time && (valueTimestamp - time) < 30000L) {
                    timeState.update(valueTimestamp);
                    ctx.timerService().deleteProcessingTimeTimer(time + 30000L);
                    ctx.timerService().registerProcessingTimeTimer(valueTimestamp + 30000L);
                }

            }

        });

        process.print("process");

        try {
            env.execute("process");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     *
     * 在普通DataStream上应用process
     *
     * @param groupId
     */
    public static void processOnNonKeyed(String groupId){

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

        SingleOutputStreamOperator<JSONObject> map = jsonText.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String text) throws Exception {
                return JSONUtils.getJSONObjectFromString(text);
            }
        });

        // 应用 process 算子
        SingleOutputStreamOperator<String> process = map.keyBy(jsonObject -> jsonObject.getInteger("customer_id")).process(new ProcessFunction<JSONObject, String>() {

            private ValueState<Long> timeState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeState", Long.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                Long valueTimestamp = DateUtils.getTimestamp(value.getString("create_time"), "yyyy-MM-dd HH:mm:ss");

                Long time = timeState.value();

                if (time == null) {
                    timeState.update(valueTimestamp);
                    ctx.timerService().registerProcessingTimeTimer(valueTimestamp + 5000L);
                }

                if ((time != null) && valueTimestamp > time && (valueTimestamp - time) < 5000L) {
                    timeState.update(valueTimestamp);
                    ctx.timerService().deleteProcessingTimeTimer(time + 5000L);
                    ctx.timerService().registerProcessingTimeTimer(valueTimestamp + 5000L);
                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception { // 普通的ProcessFunction 没有 ctx.getCurrentKey()方法
                super.onTimer(timestamp, ctx, out);

                String warnString = "触发时间 ：" + timestamp;
                out.collect(warnString);
                timeState.clear();

            }

        });

        process.print("process");

        try {
            env.execute("process");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
