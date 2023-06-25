package com.pgman.goku.table.sql.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class FlinkSQLWindowDemo {

    public static void main(String[] args) {
//        windowDemo();
        lookUpJoinForJDBC();
    }


    public static void windowDemo(){

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        tableEnvironment.executeSql("create table tbl_order(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    actual_price        double          comment '实付交易额',\n" +
                "    discount_price      double          comment '折扣金额',\n" +
                "    create_time         timestamp(3)    comment '下单时间',\n" +
                "    -- 声明 create_time 是事件时间属性，并且用 延迟 5 秒的策略来生成 watermark\n" +
                "    watermark for create_time as create_time - interval '5' second\n" +
                ") with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'topic' = 'order_topic',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")\n" +
                ";");

        tableEnvironment.executeSql("create table tbl_window_order_indi(\n" +
                "    window_start         timestamp(3) not null  comment '窗口开始时间',\n" +
                "    original_amt         double         comment '总原始交易额',\n" +
                "    actual_amt           double         comment '总实付交易额',\n" +
                "    order_cnt            bigint            comment '订单量',\n" +
                "    PRIMARY KEY (window_start) NOT ENFORCED \n" +
                ") with (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/chavin',\n" +
                "   'driver' = 'com.mysql.jdbc.Driver',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'mysql',\n" +
//                "'sink.buffer-flush.interval'='5s'," +
                "   'table-name' = 'tbl_window_order_indi'" +
                ")");


        tableEnvironment.executeSql(
                "insert into tbl_window_order_indi\n" +
                        "select\n" +
                        "    window_start,\n" +
                        "    sum(original_price) as original_amt,\n" +
                        "    sum(actual_price) as actual_amt,\n" +
                        "    count(distinct order_id) as order_cnt\n" +
                        "from table(tumble(TABLE tbl_order,descriptor(create_time),interval '10' second))\n" +
                        "where create_time >= '2023-06-04 16:40:00'\n" +
                        "group by\n" +
                        "    window_start\n" +
                        ";\n");
    }



    public static void lookUpJoinForJDBC(){

        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT,9999);


        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode().withConfiguration(configuration)
                .build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);


        tableEnvironment.executeSql("create table tbl_order(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    actual_price        double          comment '实付交易额',\n" +
                "    discount_price      double          comment '折扣金额',\n" +
                "    create_time         timestamp(3)    comment '下单时间',\n" +
                "    watermark for create_time as create_time \n" +
                ") with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'topic' = 'order_topic',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")");

        tableEnvironment.executeSql("create TEMPORARY table tbl_book_shop_mysql(\n" +
                "    shop_id        int          comment '书店ID',\n" +
                "    shop_name      string       comment '书店名称',\n" +
                "    shop_address   string       comment '书店地址',\n" +
                "    level_id       int          comment '书店等级',\n" +
                "    level_name     string       comment '书店等级名称',\n" +
                "    city_id        int          comment '城市ID',\n" +
                "    city_name      string       comment '城市名称',\n" +
                "    open_date      string       comment '开店日期',\n" +
                "    brand_id       int          comment '品牌ID',\n" +
                "    brand_name     string       comment '品牌名称',\n" +
                "    manager_id     int          comment '经理ID',\n" +
                "    manager_name   int          comment '经理名称',\n" +
                "    create_time    timestamp(3) comment '创建时间',\n" +
                "    primary key(shop_id) not enforced,\n" +
                "    watermark for create_time as create_time\n" +
                ") with (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/chavin',\n" +
                "   'driver' = 'com.mysql.jdbc.Driver',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'mysql',\n" +
                "   'table-name' = 'tbl_book_shop'\n" +
                ")");


        Table table = tableEnvironment.sqlQuery("select \n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.shop_id,\n" +
                "    t1.user_id,\n" +
                "    t2.shop_name,\n" +
                "    t2.city_id,\n" +
                "    t2.city_name \n" +
                "\n" +
                "from tbl_order t1 \n" +
                "join tbl_book_shop_mysql FOR SYSTEM_TIME AS OF t1.create_time as t2 on t1.shop_id = t2.shop_id \n" +
//                "join tbl_book_shop_mysql t2 on t1.shop_id = t2.shop_id\n" +
                "where t1.shop_id <= 10\n" +
                ";\n");



    }
}
