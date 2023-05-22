package com.pgman.goku.stream;

import com.alibaba.fastjson.JSONObject;
import com.goku.mapper.OrderMapper;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;

import java.util.ArrayList;
import java.util.List;

public class StreamDebugDemo {

    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {
        mapTest();
    }

    public static void mapTest() throws Exception {

        // 算子1 kafka source
        DataStream<String> kafkaSource = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers(ConfigurationManager.getString("broker.list"))
                        .setTopics(ConfigurationManager.getString("order.topics")) // 订阅topic信息
                        .setGroupId("my-group-test-09")
                        .setStartingOffsets(OffsetsInitializer.latest()) // 设置消费位移
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setProperty("partition.discovery.interval.ms", "10000") // 每 10 秒检查一次新分区
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source Demo"
        )
                .setParallelism(2)
                ;


        DataStream<JSONObject> operator =
                kafkaSource.map(s -> JSONObject.parseObject(s)).setParallelism(1)
                .filter(jsonObject -> Integer.valueOf(jsonObject.getString("user_id")) % 2 == 0).setParallelism(1);


        operator.print().setParallelism(2);

        //        获取关键数据结构信息
//        StreamGraph streamGraph = env.getStreamGraph();
//        JobGraph jobGraph = streamGraph.getJobGraph();

        env.execute();

    }
}
