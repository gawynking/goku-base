package com.gawyn.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WindowJoinDemo {

    public static void main(String[] args) {

//        fromKafkaJoinDemo();

        fromUpsertKafkaJoinDemo();
    }


    public static void fromKafkaJoinDemo(){
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

        tableEnvironment.executeSql("create table tbl_order_payment_source(\n" +
                "    pay_id              int             comment '支付ID',\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    payment_amount      double          comment '支付金额',\n" +
                "    payment_status      int             comment '支付状态: 0-未支付 1-支付成功 2-支付失败',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '10' second\n" +
                ")with(\n" +
                "     'connector' = 'kafka',\n" +
                "     'topic' = 'order_payment_source_topic',\n" +
                "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "     'properties.group.id' = 'testGroup',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " );");

        tableEnvironment.executeSql("select\n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.shop_id,\n" +
                "    t1.user_id,\n" +
                "    t1.original_price,\n" +
                "    t2.payment_amount,\n" +
                "    coalesce(t1.window_start, t2.window_start) as window_start,\n" +
                "    coalesce(t1.window_end, t2.window_end) as window_end\n" +
                "\n" +
                "from (\n" +
                "    select *\n" +
                "    from table(tumble(table tbl_order_source,descriptor(create_time),interval '5' minutes))\n" +
                ") t1\n" +
                "left join (\n" +
                "    select *\n" +
                "    from table(tumble(table tbl_order_payment_source,descriptor(create_time),interval '5' minutes))\n" +
                ") t2\n" +
                "     on t1.order_id = t2.order_id\n" +
                "    and t1.window_start = t2.window_start\n" +
                "    and t1.window_end = t2.window_end\n" +
                ";").print();


    }

    public static void fromUpsertKafkaJoinDemo(){
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

        tableEnvironment.executeSql("create table tbl_order_payment_source(\n" +
                "    pay_id              int             comment '支付ID',\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    payment_amount      double          comment '支付金额',\n" +
                "    payment_status      int             comment '支付状态: 0-未支付 1-支付成功 2-支付失败',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '10' second\n" +
                ")with(\n" +
                "     'connector' = 'kafka',\n" +
                "     'topic' = 'order_payment_source_topic',\n" +
                "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "     'properties.group.id' = 'testGroup',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " );");


        tableEnvironment.executeSql("create table tbl_order_source1(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '10' second,\n" +
                "    primary key(order_id) not enforced \n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'order_source_topic1',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_order_payment_source1(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    payment_amount      double          comment '支付金额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '10' second,\n" +
                "    primary key(order_id) not enforced \n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'order_payment_source_topic1',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");


        tableEnvironment.executeSql("insert into tbl_order_source1\n" +
                "select \n" +
                "\n" +
                "    order_id,\n" +
                "    sum(original_price) as original_price,\n" +
                "    max(create_time)    as create_time\n" +
                "\n" +
                "from tbl_order_source \n" +
                "group by order_id\n" +
                ";");

        tableEnvironment.executeSql("insert into tbl_order_payment_source1\n" +
                "select \n" +
                "\n" +
                "    order_id,\n" +
                "    sum(payment_amount) as payment_amount,\n" +
                "    max(create_time)    as create_time\n" +
                "\n" +
                "from tbl_order_payment_source \n" +
                "group by order_id\n" +
                ";");


        tableEnvironment.executeSql("select\n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.original_price,\n" +
                "    t2.payment_amount,\n" +
                "    coalesce(t1.window_start, t2.window_start) as window_start,\n" +
                "    coalesce(t1.window_end, t2.window_end) as window_end\n" +
                "\n" +
                "from (\n" +
                "    select *\n" +
                "    from table(tumble(table tbl_order_source,descriptor(create_time),interval '5' minutes))\n" +
                ") t1\n" +
                "left join (\n" +
                "    select *\n" +
                "    from table(tumble(table tbl_order_payment_source,descriptor(create_time),interval '5' minutes))\n" +
                ") t2\n" +
                "     on t1.order_id = t2.order_id\n" +
                "    and t1.window_start = t2.window_start\n" +
                "    and t1.window_end = t2.window_end\n" +
                ";").print();

    }

}
