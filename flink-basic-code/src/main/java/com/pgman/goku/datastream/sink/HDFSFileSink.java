package com.pgman.goku.datastream.sink;

import akka.remote.WireFormats;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HDFSFileSink {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        String topicName = "pgman";
        String groupId = "flink-pgman-test-kafka-string";

        // 1 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 定义kafka参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "test.server:9092");
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 注册kafka数据源
        DataStreamSource<String> kafkaString = env.addSource(new FlinkKafkaConsumer011<String>(topicName, new SimpleStringSchema(), properties));

        // 定义HDFS File Sink
        final StreamingFileSink<String> hdfsSink = StreamingFileSink
                .forRowFormat(
                        new Path("hdfs://192.168.72.129:8020/hdfs_sink"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(15)) // 定义不活动分桶时间
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(15)) // 定义分桶间隔时间
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build()
                )
                .withBucketCheckInterval(TimeUnit.SECONDS.toMillis(15)) // 定义检查间隔时间
                .build();


        kafkaString.addSink(hdfsSink);

        env.execute("HDFSFileSink");


    }

}
