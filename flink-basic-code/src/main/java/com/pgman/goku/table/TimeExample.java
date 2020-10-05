package com.pgman.goku.table;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.pojo.Order;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Properties;

/**
 *  1 为流指定时间语义
 *      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
 *
 *  2 定义时间属性
 *      Processing time ：
 *          1、Defining in create table DDL ：通过 PROCTIME() 在建表语句中定义 计算列实现
 *          2、During DataStream-to-Table Conversion ：通过 xxx.proctime 在模式末尾指定
 *          3、Using a TableSource ：通过自定义TableSource实现 DefinedProctimeAttribute 接口定义
 *      Event time ：
 *          1、Defining in create table DDL ：通过 建表语句中的 WATERMARK 子句定义event time属性
 *          2、During DataStream-to-Table Conversion ：通过 xxx.rowtime 在DataStream转换为表过程中定义，在定义事件时间之前需要分配时间戳和定义水位线
 *          3、Using a TableSource ：通过自定义 TableSource 实现 DefinedRowtimeAttributes 接口定义
 *
 *
 */
public class TimeExample {

    public static void main(String[] args) {

        String groupId = "time-order";



    }


    /**
     * 通过自定义 TableSource 实现 DefinedProctimeAttribute 方式定义 Processing time 属性
     *
     * @param groupId
     */
    public static void customProcessTimeFromTableSource(String groupId){

        // 1 初始化 Table 执行环境
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 2 定义数据源 及 数据结构
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConfigurationManager.getString("broker.list"));
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 注册kafka数据源
        DataStreamSource<String> jsonText = fsEnv.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        SingleOutputStreamOperator<Order> mapDataStream = jsonText.map(new MapFunction<String, Order>() {

            @Override
            public Order map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);

                Order order = new Order();

                order.setId(json.getInteger("id"));
                order.setCustomerId(json.getInteger("customer_id"));
                order.setOrderStatus(json.getInteger("order_status"));
                order.setOrderAmt(json.getInteger("order_amt"));
                order.setCreateTime(json.getString("create_time"));

                return order;
            }

        });






        // 5 启动任务
        try {
            fsTableEnv.execute("Flink Table API");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    /**
     * 内部类 ： 自定义 Order Source ，同时指定 时间属性
     */
    public static class CustomOrderSourceWithProcessTime implements StreamTableSource<Row>,DefinedProctimeAttribute{

        @Override
        public TableSchema getTableSchema() {
            return null;
        }

        @Nullable
        @Override
        public String getProctimeAttribute() {
            return null;
        }

        @Override
        public DataStream<Row> getDataStream(StreamExecutionEnvironment fsEnv) {
            return null;
        }

    }

    /**
     * 内部类 ：通过自定义 TableSource 方式实现自定义 时间时间 属性
     */
    public static class CustomOrderSourceWithEventTime implements StreamTableSource<Row>,DefinedRowtimeAttributes{
        @Override
        public DataType getProducedDataType() {
            return null;
        }

        @Override
        public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
            return null;
        }

        @Override
        public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
            return null;
        }

        @Override
        public TableSchema getTableSchema() {
            return null;
        }
    }

}
