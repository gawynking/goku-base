package com.pgman.goku.stream;

import com.alibaba.fastjson.JSONObject;
import com.goku.mapper.OrderMapper;
import com.goku.util.ObjectUtils;
import com.pgman.goku.config.ConfigurationManager;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 本示例用来演示connector format场景，主要包括以下
 *  - JSON Format
 */
public class ConnectorFormatExample {

    public static void main(String[] args) throws Exception{
        jsonStringToObject();
    }


    /**
     * JSON字符串转换为OrderMapper对象
     * @throws Exception
     */
    public static void jsonStringToObject() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(ConfigurationManager.getString("broker.list"))
                .setTopics(ConfigurationManager.getString("order.topics")) // 订阅topic信息
                .setGroupId("my-group-test-03")
                .setStartingOffsets(OffsetsInitializer.latest()) // 设置消费位移
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "10000") // 每 10 秒检查一次新分区
                .build();

        DataStream<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


        JsonSerializationSchema<OrderMapper> jsonFormat1 = new JsonSerializationSchema<>();

        JsonSerializationSchema<OrderMapper> jsonFormat2 = new JsonSerializationSchema<>(
                () -> new ObjectMapper()
                        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
        );

        KafkaSink<OrderMapper> sink = KafkaSink.<OrderMapper>builder()
                .setBootstrapServers(ConfigurationManager.getString("broker.list"))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // 至少一次语义
                .setRecordSerializer(
                        new KafkaRecordSerializationSchemaBuilder<>()
                                .setTopic("test_topic")
                                .setValueSerializationSchema(jsonFormat1)
                                .build()
                )
                .build();

        kafkaSource.map(new MapFunction<String, OrderMapper>() {
            @Override
            public OrderMapper map(String str) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(str);
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
        }).sinkTo(sink);

        env.execute();

    }


}
