package com.pgman.goku.stream;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 该示例用来演示JDBC连接器基本使用方法
 * 已创建的 JDBC Sink 能够保证至少一次的语义。 更有效的精确执行一次可以通过 upsert 语句或幂等更新实现。
 * 官档连接: https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/connectors/datastream/jdbc/
 */
public class JdbcConnectorExample {

    public static void main(String[] args) throws Exception {
        flink116JdbcConnector();
    }


    /**
     * JDBC Sink示例
     *
     * @throws Exception
     */
    public static void flink116JdbcConnector() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(ConfigurationManager.getString("broker.list"))
                .setTopics(ConfigurationManager.getString("order.topics")) // 订阅topic信息
                .setGroupId("my-group-test-02")
                .setStartingOffsets(OffsetsInitializer.latest()) // 设置消费位移
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "10000") // 每 10 秒检查一次新分区
                .build();

        DataStream<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        kafkaSource
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String str) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(str);
                        return jsonObject;
                    }
                })
                .addSink(
                    JdbcSink.sink(
                            "insert into mydb.tbl_order(order_id,shop_id,user_id,original_price,actual_price,discount_price,create_time) values (?,?,?,?,?,?,?)",
                            new JdbcStatementBuilder<JSONObject>() {
                                @Override
                                public void accept(PreparedStatement ps, JSONObject t) throws SQLException {
                                    ps.setInt(1, t.getInteger("order_id"));
                                    ps.setInt(2, t.getInteger("shop_id"));
                                    ps.setInt(3, t.getInteger("user_id"));
                                    ps.setDouble(4, t.getDouble("original_price"));
                                    ps.setDouble(5, t.getDouble("actual_price"));
                                    ps.setDouble(6, t.getDouble("discount_price"));
                                    ps.setString(7, t.getString("create_time"));
                                }
                            },
                            JdbcExecutionOptions
                                    .builder()
                                    .withBatchSize(3)
                                    .withBatchIntervalMs(10000)
                                    .build(),
                            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                    .withUrl(ConfigurationManager.getString("jdbc.url")) // jdbc url
                                    .withDriverName(ConfigurationManager.getString("jdbc.driver"))
                                    .withUsername(ConfigurationManager.getString("jdbc.user"))
                                    .withPassword(ConfigurationManager.getString("jdbc.password"))
                                    .build()
                    )
                );

        env.execute();

    }

}
