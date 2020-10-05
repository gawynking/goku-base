package com.pgman.goku.table;


import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.pojo.Order;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;


/**
 * flink Tbable创建Table 方法
 */
public class CreateTableExample {

    public static void main(String[] args) {

        String groupId = "create-table";

        createFlinkTableFromDataStreamWithPOJO(groupId);

//        createFlinkTableFromDataStreamWithJSONObject(groupId);

    }


    /**
     * 从 POJO类型的DataStream 创建Table
     *
     * @param groupId
     */
    public static void createFlinkTableFromDataStreamWithPOJO(String groupId){

        // 1 初始化 Table 执行环境
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);


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

        // 3 Create Table

        // 3.1 通过 StreamTableEnvironment.fromDataStream() 创建Table对象
        Table table1 = fsTableEnv.fromDataStream(mapDataStream);
//        table1.printSchema();

        // 3.2 通过 StreamTableEnvironment.fromDataStream() 创建Table对象 同时为列指定别名
        Table table2 = fsTableEnv.fromDataStream(mapDataStream, "id,customerId as customer_id,orderStatus as order_status,orderAmt as order_amt,createTime as create_time");
//        table2.printSchema();

        // 3.3 创建临时视图 从已存在的 Table
        fsTableEnv.createTemporaryView("table1",table1);
        fsTableEnv.from("table1").select("id,customerId as customer_id,orderStatus as order_status,orderAmt as order_amt,createTime as create_time").printSchema();

        // 3.4 创建临时视图 从 DataStream
        fsTableEnv.createTemporaryView("table1_from_datastream",mapDataStream);
        fsTableEnv.sqlQuery("select * from table1_from_datastream").printSchema();

        // 3.5 创建临时视图 从 DataStream 同时指定 列别名
        fsTableEnv.createTemporaryView("table2_from_datastream",mapDataStream,"id,customerId as customer_id,orderStatus as order_status,orderAmt as order_amt,createTime as create_time");
        fsTableEnv.sqlQuery("select * from table2_from_datastream").printSchema();


        // 4 使用SQL查询数据
        Table table = fsTableEnv.sqlQuery("select * from table1 where customerId >=3 and customerId <= 6");
        fsTableEnv.toAppendStream(table,Order.class).print("sql query result");

        // 5 将Table 转换为 DataStream
        // 5.1
        DataStream<Order> orderDataStream = fsTableEnv.toAppendStream(table1, Order.class);

        // 5.2 解析数据时需要 getSchema确定字段位置
        DataStream<Row> rowDataStream = fsTableEnv.toAppendStream(table1, Row.class);
        SingleOutputStreamOperator<JSONObject> map = rowDataStream.map(new MapFunction<Row, JSONObject>() {
            @Override
            public JSONObject map(Row row) throws Exception {
                JSONObject json = new JSONObject();
                json.put("id", row.getField(2));
                json.put("customer_id", row.getField(1));
                json.put("order_status", row.getField(4));
                json.put("order_amt", row.getField(3));
                json.put("create_time", row.getField(0));

                return json;
            }
        });
//        map.print("table to dataStream with row");

        // 5.3
        TupleTypeInfo<Tuple5> schemaTypeInfo = new TupleTypeInfo<>(
                Types.STRING,
                Types.INT,
                Types.INT,
                Types.INT,
                Types.INT
        );
        DataStream<Tuple5> tuple5DataStream = fsTableEnv.toAppendStream(table1, schemaTypeInfo);
//        tuple5DataStream.print("TupleTypeInfo");

        // 5.4
        DataStream<Tuple2<Boolean, Order>> tuple2DataStream = fsTableEnv.toRetractStream(table1, Order.class);
//        tuple2DataStream.print("RetractStream");


        try {
            fsTableEnv.execute("create table");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }




    /**
     * 创建Table 从 JSONObject类型的DataStream
     *
     * @param groupId
     */
    public static void createFlinkTableFromDataStreamWithJSONObject(String groupId){

        // 1 初始化 Table 执行环境
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);


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

        SingleOutputStreamOperator<JSONObject> mapDataStream = jsonText.map(new MapFunction<String, JSONObject>() {

            @Override
            public JSONObject map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);

                return json;
            }

        });


        // 3 Create Table
        // 3.1
        Table table1 = fsTableEnv.fromDataStream(mapDataStream);
        table1.printSchema();

        /**
         * 结果 ：
         * root
         * |-- f0: LEGACY('RAW', 'ANY<com.alibaba.fastjson.JSONObject>')
         *
         * 理解： 此种方式定义的Table 如果想要细化操作json的内部数据，应该需要自定义UDF配合使用
         *
         */


    }



}
