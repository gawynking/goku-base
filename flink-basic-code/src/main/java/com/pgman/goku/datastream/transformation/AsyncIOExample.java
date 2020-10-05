package com.pgman.goku.datastream.transformation;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.HBaseUtils;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 用于访问外部IO设备的async IO编程接口 : https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/stream/operators/asyncio.html
 *
 * Flink 的异步 I/O API 允许用户在流处理中使用异步请求客户端。API 处理与数据流的集成，同时还能处理好顺序、事件时间和容错等。
 *
 * 在具备异步数据库客户端的基础上，实现数据流转换操作与数据库的异步 I/O 交互需要以下三部分：
 *
 *     实现分发请求的 AsyncFunction
 *     获取数据库交互的结果并发送给 ResultFuture 的 回调 函数
 *     将异步 I/O 操作应用于 DataStream 作为 DataStream 的一次转换操作。
 *
 * 结果的顺序
 *     AsyncFunction 发出的并发请求经常以不确定的顺序完成，这取决于请求得到响应的顺序。 Flink 提供两种模式控制结果记录以何种顺序发出。
 *     无序模式： 异步请求一结束就立刻发出结果记录。 流中记录的顺序在经过异步 I/O 算子之后发生了改变。 当使用 处理时间 作为基本时间特征时，
 *     这个模式具有最低的延迟和最少的开销。 此模式使用 AsyncDataStream.unorderedWait(...) 方法。
 *     有序模式: 这种模式保持了流的顺序。发出结果记录的顺序与触发异步请求的顺序（记录输入算子的顺序）相同。为了实现这一点，算子将缓冲一个结果记录直到这条记录前面的所有记录都发出（或超时）。
 *     由于记录或者结果要在 checkpoint 的状态中保存更长的时间，所以与无序模式相比，有序模式通常会带来一些额外的延迟和 checkpoint 开销。此模式使用 AsyncDataStream.orderedWait(...) 方法。
 *
 * 事件时间
 *     当流处理应用使用事件时间时，异步 I/O 算子会正确处理 watermark。对于两种顺序模式，这意味着以下内容：
 *     无序模式： Watermark 既不超前于记录也不落后于记录，即 watermark 建立了顺序的边界。 只有连续两个 watermark 之间的记录是无序发出的。 在一个 watermark 后面生成的记录只会在这个 watermark 发出以后才发出。
 *     在一个 watermark 之前的所有输入的结果记录全部发出以后，才会发出这个 watermark。
 *     这意味着存在 watermark 的情况下，无序模式 会引入一些与有序模式 相同的延迟和管理开销。开销大小取决于 watermark 的频率。
 *     有序模式： 连续两个 watermark 之间的记录顺序也被保留了。开销与使用处理时间 相比，没有显著的差别。
 *     请记住，摄入时间 是一种特殊的事件时间，它基于数据源的处理时间自动生成 watermark。
 *
 * 小提示：async IO 结合 google Cache 缓存小维表到本地 可以进一步提升性能
 *
 */
public class AsyncIOExample {

    public static void main(String[] args) {

        String groupId = "async-io-order";

//        asyncWithRichFunctionUnorderedHbaseRequest(groupId);

        asyncWithRichFunctionOrderedHbaseRequest(groupId);

//        asyncUnorderedHbaseRequest(groupId);

//        asyncOrderedHbaseRequest(groupId);

    }


    /**
     * 异步IO 方式与 Hbase交互,无序版本
     *
     * @param groupId
     */
    public static void asyncWithRichFunctionUnorderedHbaseRequest(String groupId){

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

        SingleOutputStreamOperator<JSONObject> mapDataStream = jsonText.map(new MapFunction<String, JSONObject>() {

            @Override
            public JSONObject map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return json;
            }

        });


        /**
         * 异步IO操作，unorderedWait 表示结果顺序可以和输入数据顺序不一致
         *
         * 下面两个参数控制异步操作：
         *
         *     Timeout： 超时参数定义了异步请求发出多久后未得到响应即被认定为失败。 它可以防止一直等待得不到响应的请求。
         *     Capacity： 容量参数定义了可以同时进行的异步请求数。 即使异步 I/O 通常带来更高的吞吐量，执行异步 I/O 操作的算子仍然可能成为流处理的瓶颈。
         *     限制并发请求的数量可以确保算子不会持续累积待处理的请求进而造成积压，而是在容量耗尽时触发反压。
         *
         * 超时处理
         *     当异步 I/O 请求超时的时候，默认会抛出异常并重启作业。 如果你想处理超时，可以重写 AsyncFunction#timeout 方法。
         *
         * 容错保证
         *     异步 I/O 算子提供了完全的精确一次容错保证。它将在途的异步请求的记录保存在 checkpoint 中，在故障恢复时重新触发请求。
         *
         * 目前，出于一致性的原因，AsyncFunction 的算子（异步等待算子）必须位于算子链的头部
         *     根据 FLINK-13063 给出的原因，目前我们必须断开异步等待算子的算子链以防止潜在的一致性问题。
         *     这改变了先前支持的算子链的行为。需要旧有行为并接受可能违反一致性保证的用户可以实例化并手工将异步等待算子
         *     添加到作业图中并将链策略设置回通过异步等待算子的 ChainingStrategy.ALWAYS 方法进行链接。
         *
         */
        SingleOutputStreamOperator<String> asyncIODataStream = AsyncDataStream.unorderedWait(
                mapDataStream,
                new RichAsyncFunction<JSONObject, String>() {

                    private transient Connection connection = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        connection = HBaseUtils.getInstance().getConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        HBaseUtils.getInstance().close(connection);
                    }

                    @Override
                    public void asyncInvoke(JSONObject input, ResultFuture<String> resultFuture) throws Exception {

                        final Boolean result = HBaseUtils.getInstance().isDataExists(connection, "dim_event_blacklist_user", input.getString("customer_id"));

                        // 设置客户端完成请求后要执行的回调函数
                        // 回调函数只是简单地把结果发给 future
                        CompletableFuture.supplyAsync(new Supplier<Boolean>() {
                            @Override
                            public Boolean get() {
                                try {
                                    return result;
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                return null;
                            }

                        }).thenAccept(new Consumer<Boolean>() {
                            @Override
                            public void accept(Boolean aBoolean) {
                                input.put("is_blacklist", aBoolean);
                                resultFuture.complete(Collections.singletonList(input.toJSONString()));
                            }
                        });

                    }

                },
                5000,
                TimeUnit.MILLISECONDS,
                500
        );

        asyncIODataStream.print("asyncIO");

        try {
            env.execute("asyncIO");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 异步IO 方式与 Hbase交互,有序版本
     *
     * @param groupId
     */
    public static void asyncWithRichFunctionOrderedHbaseRequest(String groupId){

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

        SingleOutputStreamOperator<JSONObject> mapDataStream = jsonText.map(new MapFunction<String, JSONObject>() {

            @Override
            public JSONObject map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return json;
            }

        });


        /**
         * 异步IO操作，unorderedWait 表示结果顺序可以和输入数据顺序不一致
         *
         * 下面两个参数控制异步操作：
         *
         *     Timeout： 超时参数定义了异步请求发出多久后未得到响应即被认定为失败。 它可以防止一直等待得不到响应的请求。
         *     Capacity： 容量参数定义了可以同时进行的异步请求数。 即使异步 I/O 通常带来更高的吞吐量，执行异步 I/O 操作的算子仍然可能成为流处理的瓶颈。
         *     限制并发请求的数量可以确保算子不会持续累积待处理的请求进而造成积压，而是在容量耗尽时触发反压。
         *
         * 超时处理
         *     当异步 I/O 请求超时的时候，默认会抛出异常并重启作业。 如果你想处理超时，可以重写 AsyncFunction#timeout 方法。
         *
         * 容错保证
         *     异步 I/O 算子提供了完全的精确一次容错保证。它将在途的异步请求的记录保存在 checkpoint 中，在故障恢复时重新触发请求。
         *
         * 目前，出于一致性的原因，AsyncFunction 的算子（异步等待算子）必须位于算子链的头部
         *     根据 FLINK-13063 给出的原因，目前我们必须断开异步等待算子的算子链以防止潜在的一致性问题。
         *     这改变了先前支持的算子链的行为。需要旧有行为并接受可能违反一致性保证的用户可以实例化并手工将异步等待算子
         *     添加到作业图中并将链策略设置回通过异步等待算子的 ChainingStrategy.ALWAYS 方法进行链接。
         *
         */
        SingleOutputStreamOperator<String> asyncIODataStream = AsyncDataStream.orderedWait(
                mapDataStream,
                new RichAsyncFunction<JSONObject, String>() {

                    private transient Connection connection = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        connection = HBaseUtils.getInstance().getConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        HBaseUtils.getInstance().close(connection);
                    }

                    @Override
                    public void asyncInvoke(JSONObject input, ResultFuture<String> resultFuture) throws Exception {

                        final Boolean result = HBaseUtils.getInstance().isDataExists(connection, "dim_event_blacklist_user", input.getString("customer_id"));

                        // 设置客户端完成请求后要执行的回调函数
                        // 回调函数只是简单地把结果发给 future
                        CompletableFuture.supplyAsync(new Supplier<Boolean>() {
                            @Override
                            public Boolean get() {
                                try {
                                    return result;
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                return null;
                            }

                        }).thenAccept(new Consumer<Boolean>() {
                            @Override
                            public void accept(Boolean aBoolean) {
                                input.put("is_blacklist", aBoolean);
                                resultFuture.complete(Collections.singletonList(input.toJSONString()));
                            }
                        });

                    }

                },
                5000,
                TimeUnit.MILLISECONDS,
                500
        );

        asyncIODataStream.print("asyncIO");

        try {
            env.execute("asyncIO");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 一般而言 ，异步IO用来和外部存储介质进行交互，普通AsyncFunction方式使用比较罕见
     * @param groupId
     */
    public static void asyncUnorderedHbaseRequest(String groupId) {

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

        SingleOutputStreamOperator<JSONObject> mapDataStream = jsonText.map(new MapFunction<String, JSONObject>() {

            @Override
            public JSONObject map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return json;
            }

        });


        /**
         * 异步IO操作，unorderedWait 表示结果顺序可以和输入数据顺序不一致
         *
         * 下面两个参数控制异步操作：
         *
         *     Timeout： 超时参数定义了异步请求发出多久后未得到响应即被认定为失败。 它可以防止一直等待得不到响应的请求。
         *     Capacity： 容量参数定义了可以同时进行的异步请求数。 即使异步 I/O 通常带来更高的吞吐量，执行异步 I/O 操作的算子仍然可能成为流处理的瓶颈。
         *     限制并发请求的数量可以确保算子不会持续累积待处理的请求进而造成积压，而是在容量耗尽时触发反压。
         *
         * 超时处理
         *     当异步 I/O 请求超时的时候，默认会抛出异常并重启作业。 如果你想处理超时，可以重写 AsyncFunction#timeout 方法。
         *
         * 容错保证
         *     异步 I/O 算子提供了完全的精确一次容错保证。它将在途的异步请求的记录保存在 checkpoint 中，在故障恢复时重新触发请求。
         *
         * 目前，出于一致性的原因，AsyncFunction 的算子（异步等待算子）必须位于算子链的头部
         *     根据 FLINK-13063 给出的原因，目前我们必须断开异步等待算子的算子链以防止潜在的一致性问题。
         *     这改变了先前支持的算子链的行为。需要旧有行为并接受可能违反一致性保证的用户可以实例化并手工将异步等待算子
         *     添加到作业图中并将链策略设置回通过异步等待算子的 ChainingStrategy.ALWAYS 方法进行链接。
         *
         */
        SingleOutputStreamOperator<String> asyncIODataStream = AsyncDataStream.unorderedWait(
                mapDataStream,
                new AsyncFunction<JSONObject, String>() {

                    @Override
                    public void asyncInvoke(JSONObject input, ResultFuture<String> resultFuture) throws Exception {

                        Connection connection = HBaseUtils.getInstance().getConnection();
                        Boolean result = HBaseUtils.getInstance().isDataExists(connection, "dim_event_blacklist", input.getString("customer_id"));
                        HBaseUtils.getInstance().close(connection);

                        CompletableFuture.supplyAsync(new Supplier<Boolean>() {
                            @Override
                            public Boolean get() {
                                try {
                                    return result;
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                return false;
                            }
                        }).thenAccept(new Consumer<Boolean>() {
                            @Override
                            public void accept(Boolean aBoolean) {
                                input.put("is_blacklist",aBoolean);
                                resultFuture.complete(Collections.singletonList(input.toJSONString()));
                            }
                        });

                    }

                },
                5000,
                TimeUnit.MILLISECONDS,
                500
        );


        asyncIODataStream.print("asyncIO");

        try {
            env.execute("asyncIO");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 一般而言 ，异步IO用来和外部存储介质进行交互，普通AsyncFunction方式使用比较罕见
     * @param groupId
     */
    public static void asyncOrderedHbaseRequest(String groupId) {

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

        SingleOutputStreamOperator<JSONObject> mapDataStream = jsonText.map(new MapFunction<String, JSONObject>() {

            @Override
            public JSONObject map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);
                return json;
            }

        });


        /**
         * 异步IO操作，unorderedWait 表示结果顺序可以和输入数据顺序不一致
         *
         * 下面两个参数控制异步操作：
         *
         *     Timeout： 超时参数定义了异步请求发出多久后未得到响应即被认定为失败。 它可以防止一直等待得不到响应的请求。
         *     Capacity： 容量参数定义了可以同时进行的异步请求数。 即使异步 I/O 通常带来更高的吞吐量，执行异步 I/O 操作的算子仍然可能成为流处理的瓶颈。
         *     限制并发请求的数量可以确保算子不会持续累积待处理的请求进而造成积压，而是在容量耗尽时触发反压。
         *
         * 超时处理
         *     当异步 I/O 请求超时的时候，默认会抛出异常并重启作业。 如果你想处理超时，可以重写 AsyncFunction#timeout 方法。
         *
         * 容错保证
         *     异步 I/O 算子提供了完全的精确一次容错保证。它将在途的异步请求的记录保存在 checkpoint 中，在故障恢复时重新触发请求。
         *
         * 目前，出于一致性的原因，AsyncFunction 的算子（异步等待算子）必须位于算子链的头部
         *     根据 FLINK-13063 给出的原因，目前我们必须断开异步等待算子的算子链以防止潜在的一致性问题。
         *     这改变了先前支持的算子链的行为。需要旧有行为并接受可能违反一致性保证的用户可以实例化并手工将异步等待算子
         *     添加到作业图中并将链策略设置回通过异步等待算子的 ChainingStrategy.ALWAYS 方法进行链接。
         *
         */
        SingleOutputStreamOperator<String> asyncIODataStream = AsyncDataStream.orderedWait(
                mapDataStream,
                new AsyncFunction<JSONObject, String>() {

                    @Override
                    public void asyncInvoke(JSONObject input, ResultFuture<String> resultFuture) throws Exception {

                        Connection connection = HBaseUtils.getInstance().getConnection();
                        Boolean result = HBaseUtils.getInstance().isDataExists(connection, "dim_event_blacklist", input.getString("customer_id"));
                        HBaseUtils.getInstance().close(connection);

                        CompletableFuture.supplyAsync(new Supplier<Boolean>() {
                            @Override
                            public Boolean get() {
                                try {
                                    return result;
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                return false;
                            }
                        }).thenAccept(new Consumer<Boolean>() {
                            @Override
                            public void accept(Boolean aBoolean) {
                                input.put("is_blacklist",aBoolean);
                                resultFuture.complete(Collections.singletonList(input.toJSONString()));
                            }
                        });

                    }

                },
                5000,
                TimeUnit.MILLISECONDS,
                500
        );


        asyncIODataStream.print("asyncIO");

        try {
            env.execute("asyncIO");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
