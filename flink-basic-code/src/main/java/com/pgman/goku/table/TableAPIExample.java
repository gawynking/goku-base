package com.pgman.goku.table;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.pojo.Order;
import com.pgman.goku.pojo.OrderList;
import com.pgman.goku.util.DateUtils;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Flink Table API 示例类
 *
 */
public class TableAPIExample {

    public static void main(String[] args) {

        String groupId = "table-api";

//        scanProjectionAndFilter(groupId);

//        columnOperations(groupId);

//        aggregations(groupId);

//        setOperations(groupId);

//        joins(groupId);

        joinByWindow(groupId);




    }


    /**
     * Scan, Projection, and Filter :
     *
     * from           : 与SQL查询中的FROM子句类似。
     * Select         : 与SQL SELECT语句类似。
     * as             : 重命名字段。
     * Where / Filter : 与SQL WHERE子句类似。 过滤掉未通过过滤谓词的行。
     *
     * @param groupId
     */
    public static void scanProjectionAndFilter(String groupId){

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


        // 3 创建Table
        Table table = fsTableEnv.fromDataStream(mapDataStream);
        fsTableEnv.createTemporaryView("orders",table);

        table.printSchema();

        // 4 执行table 算子 : Scan, Projection, and Filter
        Table order = fsTableEnv
                .from("orders")
                .as("create_time,customer_id,id,order_amt,order_status")
                .where("order_status === 4")
                .filter("customer_id % 2 === 0")
                .select("id,customer_id as cust_id,order_status,order_amt,create_time");

        order.printSchema();

        fsTableEnv.toAppendStream(order, Row.class).print("Scan, Projection, and Filter");

        // 5 启动任务
        try {
            fsTableEnv.execute("Flink Table API");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * Column Operations :
     *
     * AddColumns           : 执行字段添加操作。如果添加的字段已经存在，它将引发异常。
     * AddOrReplaceColumns  : 执行字段添加操作。如果添加列名称与现有列名称相同，则现有字段将被替换。此外，如果添加的字段具有重复的字段名称，则使用最后一个。
     * DropColumns          : 执行字段删除操作。字段表达式应该是字段引用表达式，并且只能删除现有字段。
     * RenameColumns        : 执行字段重命名操作。字段表达式应该是别名表达式，并且只有现有字段可以重命名。
     *
     * @param groupId
     */
    public static void columnOperations(String groupId){

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


        // 3 创建Table
        Table table = fsTableEnv.fromDataStream(mapDataStream);
        fsTableEnv.createTemporaryView("orders",table);

        table.printSchema();

        // 4 执行算子操作
        Table result = fsTableEnv
                .from("orders")
                .addColumns("'chavin' as king")
                .addColumns("concat('customer_id',':',customerId)")
                .addOrReplaceColumns("customerId as customer_id")
                .addOrReplaceColumns("concat('king:',createTime) as create_time")
                .dropColumns("createTime")
                .renameColumns("orderStatus as order_status,orderAmt as order_amt")
                .select("*");

        result.printSchema();

        fsTableEnv.toAppendStream(result,Row.class).print("columns operation");

        // 5 启动任务
        try {
            fsTableEnv.execute("Flink Table API");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * Aggregations :
     *
     *  GroupBy Aggregation          : 与SQL GROUP BY子句类似。结果更新模式。
     *      注意：对于流式查询，计算查询结果所需的状态可能会无限增长，具体取决于聚合类型和不同分组键的数量。请提供具有有效保留间隔的查询配置，以防止状态过大。
     *  GroupBy Window Aggregation   : 对组窗口上的表以及可能的一个或多个分组键进行分组和聚合。
     *  Over Window Aggregation      : 类似于SQL OVER子句。基于前一行和后一行的窗口（范围）计算每行的窗口聚合。
     *      注意：必须在同一窗口中定义所有聚合，即相同的分区，排序和范围。目前，仅支持具有PRREDING（UNBOUNDED和有界）到CURRENT ROW范围的窗口。尚不支持使用FOLLOWING的范围。必须在单个时间属性上指定ORDER BY 。
     *  Distinct Aggregation         : 类似于SQL DISTINCT聚合子句，例如COUNT（DISTINCT a）。不同聚合声明聚合函数（内置或用户定义）仅应用于不同的输入值。Distinct可以应用于GroupBy聚合，GroupBy窗口聚合和Over Window Aggregation。
     *      注意：对于流式查询，计算查询结果所需的状态可能会无限增长，具体取决于不同字段的数量。请提供具有有效保留间隔的查询配置，以防止状态过大。
     *  Distinct                     : 与SQL DISTINCT子句类似。返回具有不同值组合的记录。
     *      注意：对于流查询，根据查询字段的数量，计算查询结果所需的状态可能会无限增长。请提供具有有效保留间隔的查询配置，以防止出现过多的状态。如果启用了状态清除功能，那么distinct必须发出消息，以防止下游运算符过早地退出状态，这会导致distinct包含结果更新。
     *
     * Table API使用window操作：
     *  1 声明时间语义
     *  2 分配时间戳和水位线
     *  3 在创建table时通过.rowtime指定时间属性
     *
     * @param groupId
     */
    public static void aggregations(String groupId){

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

        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)) {
            @Override
            public long extractTimestamp(Order element) {
                return DateUtils.getTimestamp(element.getCreateTime(),"yyyy-MM-dd HH:mm:ss");
            }
        });


        // 3 创建Table 同时指定 事件时间字段 eventTime.rowtime
        // 3.1
        Table table = fsTableEnv.fromDataStream(mapDataStream,"createTime,customerId,id,orderAmt,orderStatus,eventTime.rowtime");
        fsTableEnv.createTemporaryView("orders",table);

        table.printSchema();



        // 4.1  GroupBy Aggregation
//        Table order1 = fsTableEnv
//                .from("orders")
//                .groupBy("customerId,orderStatus")
//                .select("customerId,orderStatus,orderAmt.sum as sum_amt");
//
//        order1.printSchema();
//
//        // groupBy算子结果以更新模式执行
//        fsTableEnv.toRetractStream(order1,Row.class).print("aggregations");



        // 4.2  GroupBy Window Aggregation
//        Table order2 = fsTableEnv
//                .from("orders")
//                .window(Tumble.over("1.minutes").on("eventTime").as("window"))
//                .groupBy("customerId,window")
//                .select("customerId,window.start,window.end,window.rowtime,orderAmt.sum as total_amt");
//
//        order2.printSchema();
//
//        // groupBy算子结果以更新模式执行
//        fsTableEnv.toAppendStream(order2,Row.class).print("aggregations");



        // 4.3 Over Window Aggregation
//        Table table3 = fsTableEnv
//                .from("orders")
//                .window(Over
//                        .partitionBy("customerId")
//                        .orderBy("eventTime")
//                        .preceding("UNBOUNDED_RANGE")
//                        .following("CURRENT_RANGE")
//                        .as("window")
//                ).select("customerId as customer_id,orderAmt.max over window as max_amt,orderAmt.min over window as min_amt");
//
//        table3.printSchema();
//
//        fsTableEnv.toAppendStream(table3,Row.class).print("aggregations");


        // 4.4 Distinct Aggregation
        // 4.1 Distinct aggregation on group by
//        Table table4 = fsTableEnv
//                .from("orders")
//                .groupBy("customerId")
//                .select("customerId as customer_id,orderAmt.sum.distinct as d_sum_amt,customerId.count.distinct as d_cnt");
//
//        table4.printSchema();
//
//        fsTableEnv.toRetractStream(table4,Row.class).print("aggregations");


        // 4.2 Distinct aggregation on time window group by
//        Table table4 = fsTableEnv
//                .from("orders")
//                .window(Tumble.over("1.minutes").on("eventTime").as("window"))
//                .groupBy("customerId,window")
//                .select("customerId as customer_id,window.rowtime,orderAmt.sum.distinct as d_sum_amt,customerId.count.distinct as d_cnt");
//
//        table4.printSchema();
//
//        fsTableEnv.toRetractStream(table4,Row.class).print("aggregations");


        // 4.3 Distinct aggregation on over window --> xxx.agg.distinct 测试未通过
//        Table table4 = fsTableEnv
//                .from("orders")
//                .window(Over
//                        .partitionBy("customerId")
//                        .orderBy("eventTime")
//                        .preceding("UNBOUNDED_RANGE")
//                        .as("window"))
//                .select("customerId as customer_id,orderAmt.avg.distinct over window,orderAmt.min over window as min_amt");
//
//        table4.printSchema();
//
//        fsTableEnv.toRetractStream(table4,Row.class).print("aggregations");


        // 4.4 用户自定义聚合函数上应用 distinct aggregation ： 忽略
        // Use distinct aggregation for user-defined aggregate functions
//        tEnv.registerFunction("myUdagg", new MyUdagg());
//        orders.groupBy("users").select("users, myUdagg.distinct(points) as myDistinctResult");


        // 4.5
        Table table5 = fsTableEnv
                .from("orders")
                .dropColumns("createTime,eventTime,id,orderAmt")
                .distinct();

        table5.printSchema();

        fsTableEnv.toRetractStream(table5,Row.class).print("aggregations");


        // 5 启动任务
        try {
            fsTableEnv.execute("Flink Table API");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * Set Operations:集合操作
     *
     *  UnionAll   ： 类似于SQL UNION ALL子句。联合两张表。两个表必须具有相同的字段类型。
     *  in         : 与SQL IN子句类似。如果表达式存在于给定的表子查询中，则返回true。子查询表必须包含一列。此列必须与表达式具有相同的数据类型。
     *      注意：对于流式查询，操作将在连接和组操作中重写。计算查询结果所需的状态可能会无限增长，具体取决于不同输入行的数量。请提供具有有效保留间隔的查询配置，以防止状态过大。
     *
     * @param groupId
     */
    public static void setOperations(String groupId){

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

        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)) {
            @Override
            public long extractTimestamp(Order element) {
                return DateUtils.getTimestamp(element.getCreateTime(),"yyyy-MM-dd HH:mm:ss");
            }
        });


        // 3 创建Table 同时指定 事件时间字段 eventTime.rowtime
        // 3.1
        Table table = fsTableEnv.fromDataStream(mapDataStream,"createTime,customerId,id,orderAmt,orderStatus,eventTime.rowtime");
        fsTableEnv.createTemporaryView("orders",table);

        table.printSchema();

        // 4 Set 操作
        // 4.1 unionAll
//        Table table1 = fsTableEnv
//                .from("orders")
//                .where("orderStatus % 2 === 0")
//                .select("'false' as flag,id,customerId,orderStatus,orderAmt");
//
//        Table table2 = fsTableEnv
//                .from("orders")
//                .where("orderStatus % 2 === 1")
//                .select("'true' as flag,id,customerId,orderStatus,orderAmt");
//
//
//        table1.printSchema();
//        table2.printSchema();
//
//        Table tableAll = table1.unionAll(table2);
//
//        tableAll.printSchema();
//
//        fsTableEnv.toAppendStream(tableAll,Row.class).print("set");


        // 4.2 in
        Table table1 = fsTableEnv
                .from("orders")
                .where("orderStatus % 2 === 0")
                .select("'false' as flag,id,customerId,orderStatus,orderAmt");

        Table table2 = fsTableEnv
                .from("orders")
                .where("orderStatus % 2 === 1")
                .select("customerId");

        table1.printSchema();
        table2.printSchema();

        Table tableIn = table1
                .select("id,customerId,orderStatus,orderAmt")
                .where("customerId.in(" + table2 + ")");

        tableIn.printSchema();

        fsTableEnv.toRetractStream(tableIn,Row.class).print("in");


        // 5 启动任务
        try {
            fsTableEnv.execute("Flink Table API");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    /**
     *
     * Joins : 非窗口join
     *
     *  Inner Join                                 : 与SQL JOIN子句类似。关联两张表。两个表必须具有不同的字段名称，并且必须通过关联操作符或使用where或filter操作符定义至少一个相等关联谓词。
     *      注意：对于流式查询，计算查询结果所需的状态可能会无限增长，具体取决于不同输入行的数量。请提供具有有效保留间隔的查询配置，以防止状态过大。
     *
     *  Outer Join                                 : 与SQL LEFT / RIGHT / FULL OUTER JOIN子句类似。关联两张表。两个表必须具有不同的字段名称，并且必须至少定义一个相等关联谓词。
     *      注意：对于流式查询，计算查询结果所需的状态可能会无限增长，具体取决于不同输入行的数量。请提供具有有效保留间隔的查询配置，以防止状态过大。
     *
     *
     * @param groupId
     */
    public static void joins(String groupId){

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


        // 3 创建Table 同时指定 事件时间字段 eventTime.rowtime
        // 3.1
        Table tbl_order = fsTableEnv.fromDataStream(orderDataStream,"createTime as create_time1,customerId as custoemr_id,id as id1,orderAmt as order_amt,orderStatus as order_status");
        fsTableEnv.createTemporaryView("orders",tbl_order);

        tbl_order.printSchema();

        // 3.2
        Table tbl_orderList = fsTableEnv.fromDataStream(orderListDataStream,"createTime as create_time2,id as id2,orderId as order_id,productId as product_id,productNum as product_num");
        fsTableEnv.createTemporaryView("order_list",tbl_orderList);

        tbl_orderList.printSchema();


        // 4 join 算子
        // 4.1 Inner Join
        // 不能引入时间语义
        Table joinResult = tbl_order
                .join(tbl_orderList)
                .where("id1 = order_id")
                .select("*");

        joinResult.printSchema();

//        fsTableEnv.toAppendStream(joinResult,Row.class).print("join");

        // 4.2 Outer Join
        // 4.2.1 left outer join
        Table leftJoinResult = tbl_order
                .leftOuterJoin(tbl_orderList)
                .where("id1 = order_id")
                .select("*");

        leftJoinResult.printSchema();

        fsTableEnv.toRetractStream(leftJoinResult,Row.class).print("left join");

        // 4.2.2 right outer join
        Table rightJoinResult = tbl_order
                .rightOuterJoin(tbl_orderList, "id1 = order_id")
                .select("*");

        rightJoinResult.printSchema();

//        fsTableEnv.toRetractStream(rightJoinResult,Row.class).print("right join");

        // 4.2.3 full outer join
        Table fullJoinResult = tbl_order
                .fullOuterJoin(tbl_orderList,"id1 = order_id")
                .select("*");

        fullJoinResult.printSchema();

//        fsTableEnv.toRetractStream(fullJoinResult,Row.class).print("full join");


        // 5 启动任务
        try {
            fsTableEnv.execute("Flink Table API");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    /**
     *
     * Joins : 窗口join
     *
     *  Time-windowed Join                         : 时间窗口关联需要至少一个等关联谓词和一个限制双方时间的关联条件。 这样的条件可以由两个适当的范围谓词（<，<=，> =，>）或单个等式谓词来定义，
     *      该单个等式谓词比较两个输入表的相同类型的时间属性（即，处理时间或事件时间）。
     *      注意：时间窗口关联是可以以流方式处理的常规关联的子集。
     *
     * @param groupId
     */
    public static void joinByWindow(String groupId){

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


        // 3 创建Table 同时指定 事件时间字段 eventTime.rowtime
        // 3.1
        Table tbl_order = fsTableEnv.fromDataStream(orderDataStream,"createTime as create_time1,customerId as custoemr_id,id as id1,orderAmt as order_amt,orderStatus as order_status,time1.rowtime");
        fsTableEnv.createTemporaryView("orders",tbl_order);

        tbl_order.printSchema();

        // 3.2
        Table tbl_orderList = fsTableEnv.fromDataStream(orderListDataStream,"createTime as create_time2,id as id2,orderId as order_id,productId as product_id,productNum as product_num,time2.rowtime");
        fsTableEnv.createTemporaryView("order_list",tbl_orderList);

        tbl_orderList.printSchema();


        // 4 join 算子
        // 4.1
        Table joinResult = tbl_order
                .join(tbl_orderList)
                .where("id1 = order_id && time1 >= time2 - 1.minutes && time1 <= time2 + 1.minutes")
                .select("order_id,custoemr_id,product_id,product_num,order_status,order_amt");

        joinResult.printSchema();

//        fsTableEnv.toRetractStream(joinResult,Row.class).print("join");

        // 4.2
        Table leftJoinResult = tbl_order
                .leftOuterJoin(tbl_orderList)
                .where("id1 = order_id && time1 >= time2 - 1.minutes && time1 <= time2 + 1.minutes")
                .select("order_id,custoemr_id,product_id,product_num,order_status,order_amt");

        leftJoinResult.printSchema();

        fsTableEnv.toAppendStream(leftJoinResult,Row.class).print("left join");


        // 5 启动任务
        try {
            fsTableEnv.execute("Flink Table API");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    /**
     *
     * Joins : flink高级join
     *
     *  Inner Join with Table Function (UDTF)      :
     *  Left Outer Join with Table Function (UDTF) :
     *  Join with Temporal Table                   :
     *
     * @param groupId
     */
    public static void joinWithFunctionAndTemporal(String groupId){

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


        // 3 创建Table 同时指定 事件时间字段 eventTime.rowtime
        // 3.1
        Table tbl_order = fsTableEnv.fromDataStream(orderDataStream,"createTime as create_time1,customerId as custoemr_id,id as id1,orderAmt as order_amt,orderStatus as order_status,time1.rowtime");
        fsTableEnv.createTemporaryView("orders",tbl_order);

        tbl_order.printSchema();

        // 3.2
        Table tbl_orderList = fsTableEnv.fromDataStream(orderListDataStream,"createTime as create_time2,id as id2,orderId as order_id,productId as product_id,productNum as product_num,time2.rowtime");
        fsTableEnv.createTemporaryView("order_list",tbl_orderList);

        tbl_orderList.printSchema();


        // 4 join 算子



        // 5 启动任务
        try {
            fsTableEnv.execute("Flink Table API");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }




}
