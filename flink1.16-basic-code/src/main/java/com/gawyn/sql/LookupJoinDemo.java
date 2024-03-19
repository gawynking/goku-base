package com.gawyn.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class LookupJoinDemo {

    public static void main(String[] args) {

//        fromKafkaJoinDemo();

        fromUpsertKafkaJoinDemo();

    }


    public static void fromKafkaJoinDemo() {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        executionEnvironment.setParallelism(1);

        tableEnvironment.executeSql("create table tbl_order_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    actual_price        double          comment '实付交易额',\n" +
                "    discount_price      double          comment '折扣金额',\n" +
                "    order_status        int             comment '订单状态: 1-提单 2-支付 3-配送 4-完单 5-取消',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss'\n" +
                "--    ,watermark for create_time as create_time - interval '10' second\n" +
                "    ,row_time AS proctime()\n" +
                ")with(\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'order_source_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_user_source(\n" +
                "    user_id           int            comment '用户ID',\n" +
                "    user_name         string         comment '用户名称',\n" +
                "    sex               int            comment '性别',\n" +
                "    account           string         comment '账号',\n" +
                "    nick_name         string         comment '昵称',\n" +
                "    city_id           int            comment '城市ID',\n" +
                "    city_name         string         comment '城市名称',\n" +
                "    crowd_type        string         comment '人群类型',\n" +
                "    register_time     string         comment '注册时间',\n" +
                "    primary key(user_id) not enforced\n" +
                ") with (\n" +
                "        'connector' = 'jdbc',\n" +
                "        'url' = 'jdbc:mysql://localhost:3306/gawyn',\n" +
                "        'driver' = 'com.mysql.jdbc.Driver',\n" +
                "        'username' = 'root',\n" +
                "        'password' = 'mysql',\n" +
                "        'table-name' = 'tbl_user'\n" +
                ");");


        // join
//        tableEnvironment.executeSql("select\n" +
//                "\n" +
//                "    t1.order_id,\n" +
//                "    t1.shop_id,\n" +
//                "    t1.user_id,\n" +
//                "    t2.user_name,\n" +
//                "    t1.original_price,\n" +
//                "    t1.create_time\n" +
//                "\n" +
//                "from tbl_order_source t1\n" +
//                "join tbl_user_source for system_time as of t1.row_time t2\n" +
//                "     on t1.user_id = t2.user_id\n" +
//                ";").print();


        // left join
        tableEnvironment.executeSql("select\n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.shop_id,\n" +
                "    t1.user_id,\n" +
                "    t2.user_name,\n" +
                "    t2.city_id,\n" +
                "    t2.city_name,\n" +
                "    t1.original_price,\n" +
                "    t1.create_time\n" +
                "\n" +
                "from tbl_order_source t1\n" +
                "left join tbl_user_source for system_time as of t1.row_time t2\n" +
                "     on t1.user_id = t2.user_id\n" +
                ";\n").print();

    }

    public static void fromUpsertKafkaJoinDemo() {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        executionEnvironment.setParallelism(1);

        tableEnvironment.executeSql("create table tbl_order_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    actual_price        double          comment '实付交易额',\n" +
                "    discount_price      double          comment '折扣金额',\n" +
                "    order_status        int             comment '订单状态: 1-提单 2-支付 3-配送 4-完单 5-取消',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss'\n" +
                "--    ,watermark for create_time as create_time - interval '10' second\n" +
                "    ,row_time AS proctime()\n" +
                ")with(\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'order_source_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_user_source(\n" +
                "    user_id           int            comment '用户ID',\n" +
                "    user_name         string         comment '用户名称',\n" +
                "    sex               int            comment '性别',\n" +
                "    account           string         comment '账号',\n" +
                "    nick_name         string         comment '昵称',\n" +
                "    city_id           int            comment '城市ID',\n" +
                "    city_name         string         comment '城市名称',\n" +
                "    crowd_type        string         comment '人群类型',\n" +
                "    register_time     string         comment '注册时间',\n" +
                "    primary key(user_id) not enforced\n" +
                ") with (\n" +
                "        'connector' = 'jdbc',\n" +
                "        'url' = 'jdbc:mysql://localhost:3306/gawyn',\n" +
                "        'driver' = 'com.mysql.jdbc.Driver',\n" +
                "        'username' = 'root',\n" +
                "        'password' = 'mysql',\n" +
                "        'table-name' = 'tbl_user'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_user_upsert(\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原价金额',\n" +
                "    row_time as proctime(),\n" +
                "    primary key (user_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'order_submit_upsert_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("insert into tbl_user_upsert \n" +
                "select \n" +
                "    user_id,\n" +
                "    sum(original_price) as original_price\n" +
                "from tbl_order_source \n" +
                "group by \n" +
                "    user_id\n" +
                ";\n");

        // join
//        tableEnvironment.executeSql("select \n" +
//                "\n" +
//                "    t1.user_id,\n" +
//                "    t2.user_name,\n" +
//                "    t2.city_id,\n" +
//                "    t2.city_name,\n" +
//                "    t1.original_price\n" +
//                "\n" +
//                "from tbl_user_upsert t1 \n" +
//                "join tbl_user_source for system_time as of t1.row_time t2\n" +
//                "     on t1.user_id = t2.user_id\n" +
//                ";").print();


        // left join
        tableEnvironment.executeSql("select \n" +
                "\n" +
                "    t1.user_id,\n" +
                "    t2.user_name,\n" +
                "    t2.city_id,\n" +
                "    t2.city_name,\n" +
                "    t1.original_price\n" +
                "\n" +
                "from tbl_user_upsert t1 \n" +
                "left join tbl_user_source for system_time as of t1.row_time t2\n" +
                "     on t1.user_id = t2.user_id\n" +
                ";").print();

    }


}
