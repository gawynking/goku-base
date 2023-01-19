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

/**
 * 用来构建Flink源码跟踪示例
 */
public class FlinkSourceCodeAnalysisExample {

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

        map1.print();

        JobExecutionResult jobClient = env.execute();

    }

}
