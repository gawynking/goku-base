package com.pgman.goku.table.sql.tenv;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkStreamTableEnvironment {

    public static void main(String[] args) {
        tenvDemo02();
    }

    /**
     * 创建方式1
     */
    public static void tenvDemo01(){
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);


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
    }

    /**
     * 创建方式2
     */
    public static void tenvDemo02(){
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

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
    }
}
