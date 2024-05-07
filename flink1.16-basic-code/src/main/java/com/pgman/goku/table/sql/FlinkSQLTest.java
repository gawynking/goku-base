package com.pgman.goku.table.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLTest {

    public static void main(String[] args) throws Exception {
        sqlTest01();
    }


    public static void sqlTest01() throws Exception {

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

        tableEnvironment.executeSql("create table tbl_order_payment_source(\n" +
                "    pay_id              int             comment '支付ID',\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    payment_amount      double          comment '支付金额',\n" +
                "    payment_status      int             comment '支付状态: 0-未支付 1-支付成功 2-支付失败',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '0' second\n" +
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


        tableEnvironment.executeSql("select \n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.shop_id,\n" +
                "    t1.user_id,\n" +
                "    t2.payment_amount\n" +
                "\n" +
                "from tbl_order_source t1 \n" +
                "join tbl_order_payment_source t2 on t1.order_id = t2.order_id \n" +
                ";").print();


    }



}
