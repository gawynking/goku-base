package com.pgman.goku.table;

import com.pgman.goku.table.udf.GetCustomerInfo;
import com.pgman.goku.table.udf.ScalarGetJSONItem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableSourceExample {

    public static void main(String[] args) {
        String groupId = "table-source-group";

//        connectKafkaWithJSON(groupId);

//        connectKafkaWithString(groupId);

        kafkaTableSourceAndSinkDemo(groupId);

//        tableFunctionExample(groupId);


    }


    /**
     * 定义kafka数据源 :JSON格式
     *
     * @param groupId
     */
    public static void connectKafkaWithJSON(String groupId){

        // 创建Flink执行环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String createSQL = "create table tbl_orders(\n" +
                "id               int    comment '主键',\n" +
                "customer_id      int    comment '客户id',\n" +
                "order_status     int    comment '订单状态',\n" +
                "order_amt        int    comment '订单金额',\n" +
                "create_time      string comment '记录创建时间 : yyyy-MM-dd HH:mm:ss',\n" +
                "row_time as to_timestamp(create_time),\n" +
                "watermark for event_time as event_time - interval '5' second\n" +
                ") with (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = '0.11',\n" +
                "'connector.topic' = 'order_topic',\n" +
                "'connector.properties.zookeeper.connect' = 'test.server:2181',\n" +
                "'connector.properties.bootstrap.servers' = 'test.server:9092',\n" +
                "'connector.properties.group.id' = 'table-source-kafka-group',\n" +
                "'connector.startup-mode' = 'latest-offset',\n" +
                "\n" +
                "'format.type' = 'json'\n" +
                ")";

        bsTableEnv.sqlUpdate(createSQL);

        bsTableEnv.from("tbl_orders").printSchema();

        Table table = bsTableEnv.sqlQuery("select t1.* from tbl_orders t1 where t1.order_status = 4");

        bsTableEnv.toAppendStream(table, Row.class).print("table source");

        // 启动Flink任务
        try {
            bsTableEnv.execute("TableSource");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 定义 kafka数据源
     *
     * @param groupId
     */
    public static void connectKafkaWithString(String groupId){

        // 创建Flink执行环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        bsTableEnv.registerFunction("get_json_object",new ScalarGetJSONItem());

        String sourceSQL = "create table tbl_order_source(\n" +
                "json string comment '事件日志',\n" +
                "row_time as to_timestamp(get_json_object(json,'$.create_time')),\n" +
                "watermark for row_time as row_time - interval '5' second\n" +
                ") with (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = '0.11',\n" +
                "'connector.topic' = 'order_topic',\n" +
                "'connector.properties.zookeeper.connect' = 'test.server:2181',\n" +
                "'connector.properties.bootstrap.servers' = 'test.server:9092',\n" +
                "'connector.properties.group.id' = 'table-source-kafka-group',\n" +
                "'connector.startup-mode' = 'latest-offset',\n" +
                "\n" +
                "'format.type' = 'csv',\n" +
                "'format.field-delimiter' = '\t'\n" +
                ")";

        String querySQL = "select \n" +
                "\n" +
                "get_json_object(t1.json,'$.id') as id,\n" +
                "get_json_object(t1.json,'$.customer_id') as customer_id,\n" +
                "get_json_object(t1.json,'$.order_status') as order_status,\n" +
                "get_json_object(t1.json,'$.order_amt') as order_amt,\n" +
                "get_json_object(t1.json,'$.create_time') as create_time\n" +
                "\n" +
                "from tbl_order_source t1 \n" +
                "where get_json_object(t1.json,'$.order_status') = '4'";

        bsTableEnv.sqlUpdate(sourceSQL);

        System.out.println("tbl_order_source : ");
        bsTableEnv.from("tbl_order_source").printSchema();

        System.out.println("list table : ");
        bsTableEnv.listTables();


        Table table = bsTableEnv.sqlQuery(querySQL);

        bsTableEnv.toAppendStream(table,Row.class).print("table source");


        // 启动Flink任务
        try {
            bsTableEnv.execute("TableSource");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     *
     * SQL 方式 ： 读kafka 写kafka 案例
     *
     * @param groupId
     */
    public static void kafkaTableSourceAndSinkDemo(String groupId){

        // 创建Flink执行环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        bsTableEnv.registerFunction("get_json_object",new ScalarGetJSONItem());

        String sourceSQL = "create table tbl_order_source(\n" +
                "\n" +
                "json string comment '事件日志',\n" +
                "row_time as to_timestamp(get_json_object(json,'$.create_time')),\n" +
                "watermark for row_time as row_time - interval '5' second\n" +
                "\n" +
                ") with (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = '0.11',\n" +
                "'connector.topic' = 'order_topic',\n" +
                "'connector.properties.zookeeper.connect' = 'test.server:2181',\n" +
                "'connector.properties.bootstrap.servers' = 'test.server:9092',\n" +
                "'connector.properties.group.id' = 'table-source-kafka-group',\n" +
                "'connector.startup-mode' = 'latest-offset',\n" +
                "\n" +
                "'format.type' = 'csv',\n" +
                "'format.field-delimiter' = '\t'\n" +
                ")";


        String sinkSQL = "create table tbl_order_sink(\n" +
                "id               string    comment '主键',\n" +
                "customer_id      string    comment '客户id',\n" +
                "order_status     string    comment '订单状态',\n" +
                "order_amt        string    comment '订单金额',\n" +
                "create_time      string    comment '记录创建时间 : yyyy-MM-dd HH:mm:ss'\n" +
                ") with (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = '0.11',\n" +
                "'connector.topic' = 'pgman',\n" +
                "'connector.properties.zookeeper.connect' = 'test.server:2181',\n" +
                "'connector.properties.bootstrap.servers' = 'test.server:9092',\n" +
                "'connector.properties.group.id' = 'testGroup-00',\n" +
                "'connector.startup-mode' = 'latest-offset',\n" +
                "\n" +
                "'format.type' = 'json'\n" +
                ")";

        String etlSQL = "insert into tbl_order_sink " +
                "select \n" +
                "\n" +
                "get_json_object(t1.json,'$.id') as id,\n" +
                "get_json_object(t1.json,'$.customer_id') as customer_id,\n" +
                "get_json_object(t1.json,'$.order_status') as order_status,\n" +
                "get_json_object(t1.json,'$.order_amt') as order_amt,\n" +
                "get_json_object(t1.json,'$.create_time') as create_time\n" +
                "\n" +
                "from tbl_order_source t1 \n" +
                "where get_json_object(t1.json,'$.order_status') = '4'";

        bsTableEnv.sqlUpdate(sourceSQL);
        bsTableEnv.sqlUpdate(sinkSQL);

//        bsTableEnv.from("tbl_order_source").printSchema();
//        bsTableEnv.from("tbl_order_sink").printSchema();

        bsTableEnv.sqlUpdate(etlSQL);

        // 启动Flink任务
        try {
            bsTableEnv.execute("TableSource");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 测试表生成函数 用于join
     *
     * @param groupId
     */
    public static void tableFunctionExample(String groupId){

        // 创建Flink执行环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        bsTableEnv.registerFunction("get_json_object",new ScalarGetJSONItem());
        bsTableEnv.registerFunction("get_customer",new GetCustomerInfo());

        String sourceSQL = "create table tbl_order_source(\n" +
                "json string comment '事件日志',\n" +
                "row_time as to_timestamp(get_json_object(json,'$.create_time')),\n" +
                "watermark for row_time as row_time - interval '5' second\n" +
                ") with (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = '0.11',\n" +
                "'connector.topic' = 'order_topic',\n" +
                "'connector.properties.zookeeper.connect' = 'test.server:2181',\n" +
                "'connector.properties.bootstrap.servers' = 'test.server:9092',\n" +
                "'connector.properties.group.id' = 'table-source-kafka-group',\n" +
                "'connector.startup-mode' = 'latest-offset',\n" +
                "\n" +
                "'format.type' = 'csv',\n" +
                "'format.field-delimiter' = '\t'\n" +
                ")";

        String querySQL = "select \n" +
                "\n" +
                "get_json_object(t1.json,'$.id') as id,\n" +
                "get_json_object(t1.json,'$.customer_id') as customer_id,\n" +
                "get_json_object(t1.json,'$.order_status') as order_status,\n" +
                "get_json_object(t1.json,'$.order_amt') as order_amt,\n" +
                "get_json_object(t1.json,'$.create_time') as create_time,\n" +
                "t2.f1 as customer_name \n" +
                "\n" +
                "from tbl_order_source t1 \n" +
                "left join lateral table(get_customer(get_json_object(t1.json,'$.customer_id'))) as t2 on true \n" +
                "where get_json_object(t1.json,'$.order_status') = '4'";

        bsTableEnv.sqlUpdate(sourceSQL);

        System.out.println("tbl_order_source : ");
        bsTableEnv.from("tbl_order_source").printSchema();

        System.out.println("list table : ");
        bsTableEnv.listTables();


        Table table = bsTableEnv.sqlQuery(querySQL);

        bsTableEnv.toRetractStream(table,Row.class).print("table source");


        // 启动Flink任务
        try {
            bsTableEnv.execute("TableSource");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }




}
