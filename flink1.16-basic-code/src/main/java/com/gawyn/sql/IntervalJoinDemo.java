package com.gawyn.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class IntervalJoinDemo {

    public static void main(String[] args) {

//        innerJoinPrint();

//        innerJoinToKafka();

//        innerJoinToUpsertKafka();

        // 不支持操作
//        innerJoinFromUpsertKafka();



//        leftJoinPrint();

//        leftJoinToKafka();

//        leftJoinToUpsertKafka();

        // 不支持操作
        leftJoinFromUpsertKafka();
    }


    public static void innerJoinPrint() {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

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
                "    t2.payment_amount \n" +
                "\n" +
                "from tbl_order_source t1\n" +
                "join tbl_order_payment_source t2\n" +
                "     on t1.order_id = t2.order_id\n" +
                "    and t1.create_time >= t2.create_time - interval '10' minute\n" +
                "    and t1.create_time <= t2.create_time\n" +
                ";").print();

    }

    public static void innerJoinToKafka() {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

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
                "    'properties.group.id' = 'testGroup11111',\n" +
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
                "     'properties.group.id' = 'testGroup22222',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " );");


        tableEnvironment.executeSql("create table tbl_order_submit_payment(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    payment_amount      double          comment '支付金额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '10' second\n" +
                ")with(\n" +
                "     'connector' = 'kafka',\n" +
                "     'topic' = 'order_submit_payment_topic',\n" +
                "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "     'properties.group.id' = 'testGroup33333',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                ");\n");


        tableEnvironment.executeSql("insert into tbl_order_submit_payment \n" +
                "select\n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.shop_id,\n" +
                "    t1.user_id,\n" +
                "    t2.payment_amount,\n" +
                "    t2.create_time\n" +
                "\n" +
                "from tbl_order_source t1\n" +
                "join tbl_order_payment_source t2\n" +
                "     on t1.order_id = t2.order_id\n" +
                "    and t1.create_time >= t2.create_time - interval '10' minute\n" +
                "    and t1.create_time <= t2.create_time\n" +
                ";\n");

        tableEnvironment.executeSql("select * from tbl_order_submit_payment").print();

    }


    public static void innerJoinToUpsertKafka() {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

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
                "    'properties.group.id' = 'testGroup11111',\n" +
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
                "     'properties.group.id' = 'testGroup22222',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " );");


        tableEnvironment.executeSql("create table tbl_order_submit_payment(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    payment_amount      double          comment '支付金额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '10' second,\n" +
                "    primary key (order_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'order_submit_payment_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");\n");


        tableEnvironment.executeSql("insert into tbl_order_submit_payment\n" +
                "select\n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.shop_id,\n" +
                "    t1.user_id,\n" +
                "    t2.payment_amount,\n" +
                "    t2.create_time\n" +
                "\n" +
                "from tbl_order_source t1\n" +
                "join tbl_order_payment_source t2\n" +
                "     on t1.order_id = t2.order_id\n" +
                "    and t1.create_time >= t2.create_time - interval '10' minute\n" +
                "    and t1.create_time <= t2.create_time\n" +
                ";\n");

        tableEnvironment.executeSql("select * from tbl_order_submit_payment").print();

    }

    /**
     * 不支持输入源配置成upsert-kafka连接器格式
     */
    public static void innerJoinFromUpsertKafka() {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

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
                "    'properties.group.id' = 'testGroup11111',\n" +
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
                "     'properties.group.id' = 'testGroup22222',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " );");


        tableEnvironment.executeSql("create table tbl_order_upsert(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    original_price      double          comment '原价金额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '10' second,\n" +
                "    primary key (order_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'order_submit_upsert_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_order_payment_upsert(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    payment_amount      double          comment '支付金额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '10' second,\n" +
                "    primary key (order_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'order_payment_upsert_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");\n");

        tableEnvironment.executeSql("insert into tbl_order_upsert\n" +
                "select\n" +
                "\n" +
                "    order_id,\n" +
                "    sum(original_price) as original_price,\n" +
                "    max(create_time) as create_time\n" +
                "\n" +
                "from tbl_order_source\n" +
                "group by order_id\n" +
                ";");

        tableEnvironment.executeSql("insert into tbl_order_payment_upsert\n" +
                "select\n" +
                "\n" +
                "    order_id,\n" +
                "    sum(payment_amount) as payment_amount,\n" +
                "    max(create_time) as create_time\n" +
                "\n" +
                "from tbl_order_payment_source\n" +
                "group by order_id\n" +
                ";\n");

        tableEnvironment.executeSql("select \n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.original_price,\n" +
                "    t2.payment_amount\n" +
                "\n" +
                "from tbl_order_upsert t1 \n" +
                "join tbl_order_payment_upsert t2 \n" +
                "     on t1.order_id = t2.order_id \n" +
                "    and t1.create_time >= t2.create_time - interval '10' minute\n" +
                "    and t1.create_time <= t2.create_time\n" +
                ";").print();

    }


    public static void leftJoinPrint() {

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
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time\n" +
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
                "    watermark for create_time as create_time\n" +
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
                "    t2.payment_amount,\n" +
                "    current_watermark(t1.create_time) as t1_wm,\n" +
                "    current_watermark(t2.create_time) as t2_wm\n" +
                "\n" +
                "from tbl_order_source t1\n" +
                "left join tbl_order_payment_source t2\n" +
                "     on t1.order_id = t2.order_id\n" +
                "    and t1.create_time >= t2.create_time - interval '10' minute\n" +
                "    and t1.create_time <= t2.create_time\n" +
                ";\n").print();

    }

    public static void leftJoinToKafka() {

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

        tableEnvironment.executeSql("create table tbl_order_submit_payment(\n" +
                "    order_id               int             comment '订单ID',\n" +
                "    shop_id                int             comment '书店ID',\n" +
                "    user_id                int             comment '用户ID',\n" +
                "    payment_amount         double          comment '支付金额',\n" +
                "    t1_create_time         string          comment 't1创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    t2_create_time         string          comment 't2创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    t1_wm                  string          comment 't1水位线: yyyy-MM-dd HH:mm:ss',\n" +
                "    t2_wm                  timestamp(3)    comment 't2水位线: yyyy-MM-dd HH:mm:ss'\n" +
                ")with(\n" +
                "     'connector' = 'kafka',\n" +
                "     'topic' = 'order_submit_payment_topic',\n" +
                "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "     'properties.group.id' = 'testGroup',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " );");

        tableEnvironment.executeSql("insert into tbl_order_submit_payment\n" +
                "select\n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.shop_id,\n" +
                "    t1.user_id,\n" +
                "    t2.payment_amount,\n" +
                "    cast(t1.create_time as string) as t1_create_time,\n" +
                "    cast(t2.create_time as string) as t2_create_time,\n" +
                "    cast(current_watermark(t1.create_time) as string) as t1_wm,\n" +
                "    current_watermark(t2.create_time) as t2_wm\n" +
                "\n" +
                "from tbl_order_source t1\n" +
                "left join tbl_order_payment_source t2\n" +
                "     on t1.order_id = t2.order_id\n" +
                "    and t1.create_time >= t2.create_time - interval '10' minute\n" +
                "    and t1.create_time <= t2.create_time\n" +
                ";");

        tableEnvironment.executeSql("select * from tbl_order_submit_payment").print();

    }

    public static void leftJoinToUpsertKafka() {

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

        tableEnvironment.executeSql("create table tbl_order_submit_payment(\n" +
                "    order_id               int             comment '订单ID',\n" +
                "    shop_id                int             comment '书店ID',\n" +
                "    user_id                int             comment '用户ID',\n" +
                "    payment_amount         double          comment '支付金额',\n" +
                "    t1_create_time         timestamp(3)    comment 't1创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    t2_create_time         string          comment 't2创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    t1_wm                  string          comment 't1水位线: yyyy-MM-dd HH:mm:ss',\n" +
                "    t2_wm                  string          comment 't2水位线: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for t1_create_time as t1_create_time - interval '10' second,\n" +
                "    primary key(order_id) not enforced \n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'order_submit_payment_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                " );");

        tableEnvironment.executeSql("insert into tbl_order_submit_payment\n" +
                "select\n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.shop_id,\n" +
                "    t1.user_id,\n" +
                "    t2.payment_amount,\n" +
                "    t1.create_time as t1_create_time,\n" +
                "    cast(t2.create_time as string) as t2_create_time,\n" +
                "    cast(current_watermark(t1.create_time) as string) as t1_wm,\n" +
                "    cast(current_watermark(t2.create_time) as string) as t2_wm\n" +
                "\n" +
                "from tbl_order_source t1\n" +
                "left join tbl_order_payment_source t2\n" +
                "     on t1.order_id = t2.order_id\n" +
                "    and t1.create_time >= t2.create_time - interval '10' minute\n" +
                "    and t1.create_time <= t2.create_time\n" +
                ";");

        tableEnvironment.executeSql("select * from tbl_order_submit_payment").print();

    }

//    不支持
    public static void leftJoinFromUpsertKafka() {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

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
                "    'properties.group.id' = 'testGroup11111',\n" +
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
                "     'properties.group.id' = 'testGroup22222',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " );");


        tableEnvironment.executeSql("create table tbl_order_upsert(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    original_price      double          comment '原价金额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '10' second,\n" +
                "    primary key (order_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'order_submit_upsert_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_order_payment_upsert(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    payment_amount      double          comment '支付金额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '10' second,\n" +
                "    primary key (order_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'order_payment_upsert_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");\n");

        tableEnvironment.executeSql("insert into tbl_order_upsert\n" +
                "select\n" +
                "\n" +
                "    order_id,\n" +
                "    sum(original_price) as original_price,\n" +
                "    max(create_time) as create_time\n" +
                "\n" +
                "from tbl_order_source\n" +
                "group by order_id\n" +
                ";");

        tableEnvironment.executeSql("insert into tbl_order_payment_upsert\n" +
                "select\n" +
                "\n" +
                "    order_id,\n" +
                "    sum(payment_amount) as payment_amount,\n" +
                "    max(create_time) as create_time\n" +
                "\n" +
                "from tbl_order_payment_source\n" +
                "group by order_id\n" +
                ";\n");

        tableEnvironment.executeSql("select \n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.original_price,\n" +
                "    t2.payment_amount\n" +
                "\n" +
                "from tbl_order_upsert t1 \n" +
                "left join tbl_order_payment_upsert t2 \n" +
                "     on t1.order_id = t2.order_id \n" +
                "    and t1.create_time >= t2.create_time - interval '10' minute\n" +
                "    and t1.create_time <= t2.create_time\n" +
                ";").print();

    }

}
