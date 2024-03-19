package com.gawyn.sql;

import com.gawyn.function.OrderTagTableFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableFunctionDemo {

    public static void main(String[] args) {

//        fromAppend();

        fromUpsert();
    }

    public static void fromAppend(){

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.createTemporarySystemFunction("exp_order_tag", OrderTagTableFunction.class);

        tableEnvironment.executeSql("create table tbl_order_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    actual_price        double          comment '实付交易额',\n" +
                "    discount_price      double          comment '折扣金额',\n" +
                "    order_status        int             comment '订单状态: 1-提单 2-支付 3-配送 4-完单 5-取消',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    order_tag           string          comment '订单标签集合',\n" +
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


        tableEnvironment.executeSql("select\n" +
                "    order_id,\n" +
                "    shop_id,\n" +
                "    user_id,\n" +
                "    tag\n" +
                "from tbl_order_source\n" +
                "join lateral table(exp_order_tag(order_tag)) t(tag) on true\n" +
                ";\n").print();

    }

    public static void fromUpsert(){

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.createTemporarySystemFunction("exp_order_tag", OrderTagTableFunction.class);

        tableEnvironment.executeSql("create table tbl_order_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    actual_price        double          comment '实付交易额',\n" +
                "    discount_price      double          comment '折扣金额',\n" +
                "    order_status        int             comment '订单状态: 1-提单 2-支付 3-配送 4-完单 5-取消',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    order_tag           string          comment '订单标签集合',\n" +
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


        tableEnvironment.executeSql("create table tbl_order_upsert(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    order_tag           string          comment '订单标签集合',\n" +
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

        tableEnvironment.executeSql("insert into tbl_order_upsert\n" +
                "select\n" +
                "    order_id,\n" +
                "    user_id,\n" +
                "    max(original_price) as original_price,\n" +
                "    order_tag,\n" +
                "    max(create_time)    as create_time\n" +
                "from tbl_order_source\n" +
                "group by\n" +
                "    order_id,\n" +
                "    user_id,\n" +
                "    order_tag\n" +
                ";");


        tableEnvironment.executeSql("select\n" +
                "    order_id,\n" +
                "    user_id,\n" +
                "    tag\n" +
                "from tbl_order_upsert\n" +
                "left join lateral table(exp_order_tag(order_tag)) t(tag) on true\n" +
                ";\n").print();

    }
}
