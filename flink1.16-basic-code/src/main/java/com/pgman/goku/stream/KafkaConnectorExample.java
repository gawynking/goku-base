package com.pgman.goku.stream;

import com.pgman.goku.config.ConfigurationManager;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 本示例用于演示Flink1.16如何连接Kafka数据源
 * 该版本kafka连接器推荐启用KafkaSource和KafkaSink实现
 * 官档连接: https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/connectors/datastream/kafka/
 */
public class KafkaConnectorExample {

    public static void main(String[] args) throws Exception {
        flink116KafkaConnector();
    }


    /**
     * Flink1.16版本连接Kafka数据源方法
     *
     * @throws Exception
     */
    public static void flink116KafkaConnector() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(ConfigurationManager.getString("broker.list"))
                .setTopics(ConfigurationManager.getString("order.topics")) // 订阅topic信息
                .setGroupId("my-group-test-01")
                .setStartingOffsets(OffsetsInitializer.earliest()) // 设置消费位移
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "10000") // 每 10 秒检查一次新分区
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(ConfigurationManager.getString("broker.list"))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                        .setTopic(ConfigurationManager.getString("order.sink.topics"))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // 至少一次语义
                .build();

        DataStream<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        kafkaSource.sinkTo(sink);

        env.execute();

    }

}
