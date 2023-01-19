package com.pgman.goku.stream;

import com.alibaba.fastjson.JSONObject;
import com.goku.mapper.OrderMapper;
import com.pgman.goku.config.ConfigurationManager;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamNode;

/**
 * map算子示例
 * map算子可以用来实现SQL select list单行转换函数功能
 */
public class MapExample {

    public static void main(String[] args) throws Exception {
        mapTest01(2);
    }

    public static void mapTest01(Integer flag) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(ConfigurationManager.getString("broker.list"))
                .setTopics(ConfigurationManager.getString("order.topics")) // 订阅topic信息
                .setGroupId("my-group-test-05")
                .setStartingOffsets(OffsetsInitializer.latest()) // 设置消费位移
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "10000") // 每 10 秒检查一次新分区
                .build();

        DataStream<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source Demo");

        /**
         * 1 匿名类实现MapFunction
         */
        DataStream<JSONObject> map1 = kafkaSource.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONObject.parseObject(s);
            }
        });

        /**
         * 2 匿名类实现MapFunction，显示定义类型
         */
        DataStream<OrderMapper> map2 = kafkaSource.map(new MapFunction<String, OrderMapper>() {
            @Override
            public OrderMapper map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                return new OrderMapper(
                        jsonObject.getInteger("order_id"),
                        jsonObject.getInteger("shop_id"),
                        jsonObject.getInteger("user_id"),
                        jsonObject.getDouble("original_price"),
                        jsonObject.getDouble("actual_price"),
                        jsonObject.getDouble("discount_price"),
                        jsonObject.getString("create_time")
                );
            }
        }, TypeInformation.of(OrderMapper.class));


        DataStream<JSONObject> map3 = kafkaSource.map(new RichMapFunction<String, JSONObject>() {

            /**
             * 运行时上下文信息及状态管理信息
             *
             * @param t
             */
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

            /**
             * 开启资源,算子启动时执行一次
             *
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public JSONObject map(String s) throws Exception {
                return null;
            }

            /**
             * 关闭资源,结束时执行一次
             *
             * @throws Exception
             */
            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        switch (flag){
            case 1:
                map1.print();
                break;
            case 2:
                map2.print();
                break;
            case 3:
                map3.print();
                break;
        }

        JobExecutionResult jobClient = env.execute();

    }

}
