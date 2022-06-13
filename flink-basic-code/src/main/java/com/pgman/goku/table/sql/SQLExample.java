package com.pgman.goku.table.sql;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.pojo.Order;
import com.pgman.goku.pojo.OrderList;
import com.pgman.goku.table.udf.ScalarGetJSONItem;
import com.pgman.goku.table.udf.ScalarGetTimestamp;
import com.pgman.goku.util.DateUtils;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Flink SQL示例类
 *
 *
 */
public class SQLExample {

    public static void main(String[] args) {

        String groupId = "sql-example";

//        createTable(groupId);

//        queryExample(groupId);

        parseBinlog();


    }


    public static void parseBinlog() {

        // 1.1 创建Flink执行环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);



        bsTableEnv.registerFunction("get_json_object",new ScalarGetJSONItem());


        String tblOrderSQL = "create table yw_order(\n" +
                "\n" +
                "json string \n" +
                "\n" +
                ") with (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = '0.11',\n" +
                "'connector.startup-mode' = 'group-offsets',\n" +
                "\n" +
                "-- 生产：\n" +
                "\n" +
                "-- 测试：\n" +
                "'connector.topic' = 'pgman',\n" +
                "'connector.properties.zookeeper.connect' = 'test.server:2181',\n" +
                "'connector.properties.bootstrap.servers' = 'test.server:9092',\n" +
                "\n" +
                "'format.type' = 'csv',\n" +
                "'format.field-delimiter' = '\t'" +
                "\n" +
                ")";

        bsTableEnv.sqlUpdate(tblOrderSQL);

        Table table = bsTableEnv.sqlQuery("select \n" +
                "\n" +
                "get_json_object(substr(get_json_object(json,'$.data'),2,char_length(get_json_object(json,'$.data'))-2),'$.id')                                                                   as clue_id,\n" +
                "get_json_object(substr(get_json_object(json,'$.data'),2,char_length(get_json_object(json,'$.data'))-2),'$.user_id')                                                              as user_id,\n" +
                "get_json_object(substr(get_json_object(json,'$.data'),2,char_length(get_json_object(json,'$.data'))-2),'$.is_distribute')                                                        as is_distribute,\n" +
                "1                                                                                                                                                                                as from_source,\n" +
                "from_unixtime(cast(get_json_object(substr(get_json_object(json,'$.data'),2,char_length(get_json_object(json,'$.data'))-2),'$.create_datetime') as bigint),'yyyy-MM-dd HH:mm:ss') as create_time,\n" +
                "from_unixtime(cast(get_json_object(substr(get_json_object(json,'$.data'),2,char_length(get_json_object(json,'$.data'))-2),'$.update_datetime') as bigint),'yyyy-MM-dd HH:mm:ss') as update_time\n" +
                "\n" +
                "from yw_order \n" +
                "where get_json_object(json,'$.type') in ('INSERT','UPDATE') \n" +
                "and get_json_object(json,'$.isDdl') = 'false' \n" +
                "and get_json_object(json,'$.table') = 'yw_order' ");

        bsTableEnv.toRetractStream(table,Row.class).print();


        try {
            bsTableEnv.execute("Flink SQL");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }



    public static void sqlView() {

        // 1.1 创建Flink执行环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // 1.2 启用检查点功能
        bsEnv.enableCheckpointing(1000);
        bsEnv.setStateBackend(new FsStateBackend("hdfs://cdh01:8020/flink/ck02"));
        bsEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        bsEnv.getCheckpointConfig().setCheckpointTimeout(5000);
        bsEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        bsEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 取消任务不清楚检查点信息

        bsEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,100));


        bsTableEnv.registerFunction("get_json_object",new ScalarGetJSONItem());



        String custSQL = "create table tbl_customer(\n" +
                "customer_id         string    comment '客户id',\n" +
                "customer_name       string    comment '客户姓名',\n" +
                "sex                 string    comment '性别',\n" +
                "address             string    comment '住址'\n" +
                ",proc_time           as        proctime()" +
//                "WATERMARK FOR proc_time AS proc_time - INTERVAL '5' SECOND" +
                ") with (\n" +
                "'connector.type' = 'jdbc',\n" +
                "'connector.url' = 'jdbc:mysql://127.0.0.1:3306/test',\n" +
                "'connector.table' = 'tbl_customer',\n" +
                "'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "'connector.username' = 'root',\n" +
                "'connector.password' = 'mysql',\n" +
                "'connector.read.fetch-size' = '1',\n" +
                "'connector.write.flush.max-rows' = '1',\n" +
                "'connector.write.flush.interval' = '1s'\n" +
                ")";


        String orderSQL = "create table tbl_order_source(\n" +
                "\n" +
                "json string comment '事件日志',\n" +
                "proc_time as proctime()" +
//                ",WATERMARK FOR proc_time AS proc_time - INTERVAL '5' SECOND" +
                "\n" +
                ") with (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = '0.11',\n" +
                "'connector.topic' = 'order_topic',\n" +
                "'connector.properties.zookeeper.connect' = 'test.server:2181',\n" +
                "'connector.properties.bootstrap.servers' = 'test.server:9092',\n" +
                "'connector.properties.group.id' = 'table-source-kafka-group100060',\n" +
                "'connector.startup-mode' = 'group-offsets',\n" +
                "\n" +
                "'format.type' = 'csv',\n" +
                "'format.field-delimiter' = '|'\n" +
                ")";


        bsTableEnv.sqlUpdate(custSQL);
        bsTableEnv.sqlUpdate(orderSQL);

        Table tblCustomer = bsTableEnv.from("tbl_customer");

        TemporalTableFunction customer = tblCustomer.createTemporalTableFunction("proc_time","customer_id");

        bsTableEnv.registerFunction("customer",customer);


        String joinSQL = "select \n" +
                "\n" +
                "get_json_object(t1.json,'$.customer_id')   as customer_id,\n" +
                "t2.customer_name                           as customer_name,\n" +
                "t2.sex                                     as sex,\n" +
                "t2.address                                 as address,\n" +
                "get_json_object(t1.json,'$.order_amt')     as order_amt,\n" +
                "get_json_object(t1.json,'$.create_time')   as create_time \n" +
                "\n" +
                "from tbl_order_source t1,\n" +
                "lateral table(customer(t1.proc_time)) AS t2 \n" +
                "where get_json_object(t1.json,'$.customer_id') = t2.customer_id";


        Table table = bsTableEnv.sqlQuery(joinSQL);

        bsTableEnv.toRetractStream(table,Row.class).print();


        try {
            bsTableEnv.execute("Flink SQL");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }



    /**
     * CREATE 语句用于向当前或指定的 Catalog 中注册表、视图或函数。注册后的表、视图和函数可以在 SQL 查询中使用。
     *
     * 目前 Flink SQL 支持下列 CREATE 语句：
     *
     *  CREATE TABLE
     *  CREATE DATABASE
     *  CREATE FUNCTION
     *
     */
    public static void createTable(String groupId){

        // 1 初始化 Table 执行环境 Blink模式
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);


        bsTableEnv.registerFunction("to_timestamp",new ScalarGetTimestamp());


        String createSQL = "create table tbl_order (\n" +
                "id int comment '主键',\n" +
                "customer_id int comment '客户id',\n" +
                "order_status int comment '订单状态',\n" +
                "order_amt int comment '订单金额',\n" +
                "create_time string comment '记录创建时间 : yyyy-MM-dd HH:mm:ss',\n" +
                "p_date as substr(create_time,1,10),\n" +
                "event_time as cast(to_timestamp(create_time) as timestamp(3)),\n" +
                "watermark for event_time as event_time\n" +
                ") comment '订单表' \n" +
                "with (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = '0.11',\n" +
                "'connector.topic' = 'topic_name',\n" +
                "'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "'connector.properties.group.id' = 'testGroup',\n" +
                "'connector.startup-mode' = 'earliest-offset',\n" +
                "'format.type' = 'json'\n" +
                ")";

        bsTableEnv.sqlUpdate(createSQL);

        bsTableEnv.listCatalogs();
        bsTableEnv.listDatabases();
        bsTableEnv.listFunctions();
        bsTableEnv.listModules();
        bsTableEnv.listTables();
        bsTableEnv.listTemporaryTables();
        bsTableEnv.listTemporaryViews();
        bsTableEnv.listUserDefinedFunctions();

        Table table = bsTableEnv.sqlQuery("select * from tbl_order");

        table.printSchema();

        bsTableEnv.toAppendStream(table,Row.class).print("sql");


        try {
            bsTableEnv.execute("Flink SQL");
        } catch (Exception e) {
            e.printStackTrace();
        }



    }


    /**
     *  Flink SQL select 示例
     *
     * @param groupId
     */
    public static void queryExample(String groupId){

        // 1 初始化 Table 执行环境
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        // 设置时间语义
        fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2 定义数据源 及 数据结构
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConfigurationManager.getString("broker.list"));
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 注册kafka数据源
        DataStreamSource<String> order = fsEnv.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        DataStreamSource<String> orderList = fsEnv.addSource(
                new FlinkKafkaConsumer011<String>(
                        ConfigurationManager.getString("order.detail.topics"),
                        new SimpleStringSchema(),
                        properties
                )
        );

        // 订单DataStream
        SingleOutputStreamOperator<Order> orderDataStream = order.map(new MapFunction<String, Order>() {

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

        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)) {
            @Override
            public long extractTimestamp(Order element) {
                return DateUtils.getTimestamp(element.getCreateTime(),"yyyy-MM-dd HH:mm:ss");
            }
        });


        // 订单明细 DataSTream
        SingleOutputStreamOperator<OrderList> orderListDataStream = orderList.map(new MapFunction<String, OrderList>() {
            @Override
            public OrderList map(String text) throws Exception {
                JSONObject json = JSONUtils.getJSONObjectFromString(text);

                OrderList orderList = new OrderList();

                orderList.setId(json.getInteger("id"));
                orderList.setOrderId(json.getInteger("order_id"));
                orderList.setProductId(json.getInteger("product_id"));
                orderList.setProductNum(json.getInteger("product_num"));
                orderList.setCreateTime(json.getString("create_time"));

                return orderList;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderList>(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)) {
            @Override
            public long extractTimestamp(OrderList element) {
                return DateUtils.getTimestamp(element.getCreateTime(), "yyyy-MM-dd HH:mm:ss");
            }
        });


        // 3 注册表
        // 3.1
        Table tbl_order = fsTableEnv.fromDataStream(orderDataStream,"createTime as create_time,customerId as customer_id,id as id,orderAmt as order_amt,orderStatus as order_status,event_time.rowtime");
        fsTableEnv.createTemporaryView("tbl_order",tbl_order);

        tbl_order.printSchema();

        // 3.2
        Table tbl_orderList = fsTableEnv.fromDataStream(orderListDataStream,"createTime as create_time,id as id,orderId as order_id,productId as product_id,productNum as product_num,event_time.rowtime");
        fsTableEnv.createTemporaryView("tbl_order_list",tbl_orderList);

        tbl_orderList.printSchema();


        // 4 执行SQL 查询
        // 4.1 普通聚合计算 ，统计历史至今所有数据
//        Table query = fsTableEnv.sqlQuery("select customer_id,sum(order_amt) from tbl_order where order_status = 4 group by customer_id");

        // 4.2 窗口分组聚合计算，窗口内统计计算
//        Table query = fsTableEnv.sqlQuery("select customer_id,sum(order_amt) as total_amt from tbl_order where order_status = 4 group by customer_id,tumble(event_time, interval '1' minute)");

        // 4.3 分组排序 窗口计算 ,流式窗口计算 不支持 倒序计算模式
//        Table query = fsTableEnv.sqlQuery("select customer_id,sum(order_amt)over(partition by customer_id order by event_time) from tbl_order where order_status = 4");

//        Table query = fsTableEnv.sqlQuery("select customer_id,create_time,sum(order_amt) over w,count(id) over w from tbl_order where order_status = 4 window w as (partition by customer_id order by event_time asc)");

        // 4.4 distinct
//        Table query = fsTableEnv.sqlQuery("select distinct customer_id,order_status,order_amt from tbl_order where order_status = 4");

        // 4.5 报表函数 Grouping sets, Rollup, Cube 流式 Grouping sets、Rollup 以及 Cube 只在 Blink planner 中支持 -- 此处测试略过
//        Table query = fsTableEnv.sqlQuery("select customer_id,order_status,sum(order_amt) as total_amt from tbl_order group by grouping sets (customer_id,order_status)");

        // 4.6 having子句
//        Table query = fsTableEnv.sqlQuery("select customer_id,sum(order_amt) from tbl_order where order_status = 4 group by customer_id having sum(order_amt) > 300");

        // 4.7 内连接 : 有效保留间隔的查询配置
//        Table query = fsTableEnv.sqlQuery("select t1.customer_id,t2.product_id,t2.product_num,t1.order_amt,t1.create_time from tbl_order t1 join tbl_order_list t2 on t1.id = t2.order_id where t1.order_status = 4");

        // 4.8 外连接
//        Table query = fsTableEnv.sqlQuery("select t1.customer_id,t2.product_id,t2.product_num,t1.order_amt,t1.create_time from tbl_order t1 left join tbl_order_list t2 on t1.id = t2.order_id where t1.order_status = 4");

//        Table query = fsTableEnv.sqlQuery("select t1.customer_id,t2.product_id,t2.product_num,t1.order_amt,t1.create_time from tbl_order t1 right join tbl_order_list t2 on t1.id = t2.order_id where t1.order_status = 4");

//        Table query = fsTableEnv.sqlQuery("select t1.customer_id,t2.product_id,t2.product_num,t1.order_amt,t1.create_time from tbl_order t1 full outer join tbl_order_list t2 on t1.id = t2.order_id where t1.order_status = 4");

        // 4.9 窗口join
        Table query = fsTableEnv.sqlQuery(
                "select t1.customer_id,t2.product_id,t2.product_num,t1.order_amt,t1.create_time " +
                        "from tbl_order t1 " +
                        "left join tbl_order_list t2 " +
                        "on t1.id = t2.order_id " +
                        "where t1.order_status = 4 " +
                        "and t1.event_time >= t2.event_time - interval '1' minute and t1.event_time <= t2.event_time + interval '1' minute"
        );

        // 4.10 表函数join -- 维表关联(流批join)主要实现技术


        query.printSchema();

        fsTableEnv.toRetractStream(query, Row.class).filter(new FilterFunction<Tuple2<Boolean, Row>>() {
            @Override
            public boolean filter(Tuple2<Boolean, Row> row) throws Exception {
                return row.f0 == true;
            }
        }).print("query");


        // 5 启动任务
        try {
            fsTableEnv.execute("Flink Table API");
        } catch (Exception e) {
            e.printStackTrace();
        }



    }



}
