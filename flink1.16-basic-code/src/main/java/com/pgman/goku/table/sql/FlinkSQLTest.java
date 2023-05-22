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

        /**
         * create table tbl_order(
         *     json   string  comment '事件日志',
         *     ts TIMESTAMP(3) METADATA FROM 'timestamp'
         * ) with (
         *     'connector' = 'kafka',
         *     'topic' = 'order_topic',
         *     'properties.bootstrap.servers' = 'localhost:9092',
         *     'properties.group.id' = 'testGroup',
         *     'scan.startup.mode' = 'earliest-offset',
         *     'format' = 'json',
         *      'json.fail-on-missing-field' = 'false',
         *      'json.ignore-parse-errors' = 'true'
         * )
         */
        tableEnvironment.executeSql("create table tbl_order(\n" +
                "    order_id            int       comment '订单ID',\n" +
                "    shop_id             int       comment '书店ID',\n" +
                "    user_id             int       comment '用户ID',\n" +
                "    original_price      double    comment '原始交易额',\n" +
                "    actual_price        double    comment '实付交易额',\n" +
                "    discount_price      double    comment '折扣金额',\n" +
                "    create_time         string    comment '下单时间',\n" +
                "    ts timestamp(3) metadata from 'timestamp' \n" +
                ") with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'order_topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                ")");

        tableEnvironment.executeSql("select * from tbl_order").print();

        executionEnvironment.execute();

    }

}
