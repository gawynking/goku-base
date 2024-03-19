package com.gawyn.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TemporalJoinDemo {

    public static void main(String[] args) {

//        fromAppendPrint();

//        fromUpsertPrint();

//        Processing-time temporal join is not supported yet.
        fromAppendProcessingTimePrint();
    }


    public static void fromAppendPrint() {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.executeSql("create table tbl_order_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    actual_price        double          comment '实付交易额',\n" +
                "    discount_price      double          comment '折扣金额',\n" +
                "    order_status        int             comment '订单状态: 1-提单 2-支付 3-配送 4-完单 5-取消',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '0' second\n" +
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
                "    register_time     timestamp(3)         comment '注册时间',\n" +
                "    watermark for register_time as register_time - interval '0' second\n" +
                ") with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'user_source_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_user_upsert(\n" +
                "    user_id           int            comment '用户ID',\n" +
                "    city_id           int            comment '城市ID',\n" +
                "    city_name         string         comment '城市名称',\n" +
                "    register_time     timestamp(3)         comment '注册时间',\n" +
                "    watermark for register_time as register_time - interval '0' second,\n" +
                "    primary key(user_id) not enforced\n" +
                ") with (\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'user_upsert_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("insert into tbl_user_upsert\n" +
                "select\n" +
                "    user_id,\n" +
                "    max(city_id)       as city_id,\n" +
                "    max(city_name)     as city_name,\n" +
                "    max(register_time) as register_time\n" +
                "from tbl_user_source\n" +
                "group by\n" +
                "    user_id\n" +
                ";\n");


        tableEnvironment.executeSql("select\n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.shop_id,\n" +
                "    t1.user_id,\n" +
                "    t2.city_id,\n" +
                "    t2.city_name,\n" +
                "    t1.original_price,\n" +
                "    t1.create_time\n" +
                "\n" +
                "from tbl_order_source t1\n" +
                "join tbl_user_upsert for system_time as of t1.create_time t2\n" +
                "     on t1.user_id = t2.user_id\n" +
                ";").print();

    }

    public static void fromUpsertPrint() {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.executeSql("create table tbl_order_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    actual_price        double          comment '实付交易额',\n" +
                "    discount_price      double          comment '折扣金额',\n" +
                "    order_status        int             comment '订单状态: 1-提单 2-支付 3-配送 4-完单 5-取消',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '10' second\n" +
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
                "    register_time     timestamp(3)         comment '注册时间',\n" +
                "    watermark for register_time as register_time - interval '10' second\n" +
                ") with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'user_source_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_order_upsert(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '10' second,\n" +
                "    primary key(order_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'order_upsert_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_user_upsert(\n" +
                "    user_id           int            comment '用户ID',\n" +
                "    city_id           int            comment '城市ID',\n" +
                "    city_name         string         comment '城市名称',\n" +
                "    register_time     timestamp(3)         comment '注册时间',\n" +
                "    watermark for register_time as register_time - interval '10' second,\n" +
                "    primary key(user_id) not enforced\n" +
                ") with (\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'user_upsert_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("insert into tbl_order_upsert\n" +
                "select\n" +
                "    order_id,\n" +
                "    user_id,\n" +
                "    max(original_price) as original_price,\n" +
                "    max(create_time)    as create_time\n" +
                "from tbl_order_source\n" +
                "group by\n" +
                "    order_id,\n" +
                "    user_id\n" +
                ";");

        tableEnvironment.executeSql("insert into tbl_user_upsert\n" +
                "select\n" +
                "    user_id,\n" +
                "    max(city_id)       as city_id,\n" +
                "    max(city_name)     as city_name,\n" +
                "    max(register_time) as register_time\n" +
                "from tbl_user_source\n" +
                "group by\n" +
                "    user_id\n" +
                ";\n");


        tableEnvironment.executeSql("select\n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.user_id,\n" +
                "    t2.city_id,\n" +
                "    t2.city_name,\n" +
                "    t1.original_price,\n" +
                "    t1.create_time\n" +
                "\n" +
                "from tbl_order_upsert t1\n" +
                "join tbl_user_upsert for system_time as of t1.create_time t2\n" +
                "     on t1.user_id = t2.user_id\n" +
                ";\n").print();

    }

    /**
     * Processing-time temporal join is not supported yet.
     */
    public static void fromAppendProcessingTimePrint() {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.executeSql("create table tbl_order_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    actual_price        double          comment '实付交易额',\n" +
                "    discount_price      double          comment '折扣金额',\n" +
                "    order_status        int             comment '订单状态: 1-提单 2-支付 3-配送 4-完单 5-取消',\n" +
                "    create_time         string          comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    row_time as proctime() \n" +
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
                "    row_time as proctime() \n" +
                ") with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'user_source_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_user_upsert(\n" +
                "    user_id           int            comment '用户ID',\n" +
                "    city_id           int            comment '城市ID',\n" +
                "    city_name         string         comment '城市名称',\n" +
                "    register_time     string         comment '注册时间',\n" +
                "    row_time as proctime() ,\n" +
                "    primary key(user_id) not enforced\n" +
                ") with (\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'user_upsert_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("insert into tbl_user_upsert\n" +
                "select\n" +
                "    user_id,\n" +
                "    max(city_id)       as city_id,\n" +
                "    max(city_name)     as city_name,\n" +
                "    max(register_time) as register_time\n" +
                "from tbl_user_source\n" +
                "group by\n" +
                "    user_id\n" +
                ";\n");


        tableEnvironment.executeSql("select\n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.shop_id,\n" +
                "    t1.user_id,\n" +
                "    t2.city_id,\n" +
                "    t2.city_name,\n" +
                "    t1.original_price,\n" +
                "    t1.create_time\n" +
                "\n" +
                "from tbl_order_source t1\n" +
                "join tbl_user_upsert for system_time as of t1.row_time t2\n" +
                "     on t1.user_id = t2.user_id\n" +
                ";").print();

    }

}
