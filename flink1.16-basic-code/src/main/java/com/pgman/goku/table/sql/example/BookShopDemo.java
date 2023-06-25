package com.pgman.goku.table.sql.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class BookShopDemo {

    public static void main(String[] args) {
        bookShopDemo();
    }

    public static void bookShopDemo(){

        /**
         * 初始化环境信息
         */
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT,8088);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .withConfiguration(configuration)
                .build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        /**
         * 创建表
         */
        // 1 创建书店表
        tableEnvironment.executeSql("create table tbl_book_shop(\n" +
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
                "    primary key(shop_id) not enforced, -- 定义主键约束，但不做强校验\n" +
                "    watermark for create_time as create_time -- 通过watermark定义事件时间  \n" +
                ") with (\n" +
                "     'connector' = 'kafka',\n" +
                "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "     'topic' = 'shop_topic',\n" +
                "     'properties.group.id' = 'testGroup',\n" +
                "     'scan.startup.mode' = 'earliest-offset',\n" +
                "     'value.format' = 'json'\n" +
                ")\n" +
                ";");
//
//        // 2 创建图书表
//        tableEnvironment.executeSql("create table tbl_book(\n" +
//                "    book_id        int            comment '图书ID',\n" +
//                "    book_name      string         comment '图书名称',\n" +
//                "    price          double         comment '图书售价',\n" +
//                "    category_id    int            comment '品类ID',\n" +
//                "    category_name  string         comment '品类名称',\n" +
//                "    author         string         comment '作者',\n" +
//                "    publisher      string         comment '出版社',\n" +
//                "    publisher_date string         comment '出版日期',\n" +
//                "    create_time    timestamp(3)   comment '创建时间',\n" +
//                "    primary key(book_id) not enforced, -- 定义主键约束，但不做强校验\n" +
//                "    watermark for create_time as create_time -- 通过watermark定义事件时间  \n" +
//                ") with (\n" +
//                "      'connector' = 'kafka',\n" +
//                "      'properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "      'topic' = 'book_topic',\n" +
//                "      'properties.group.id' = 'testGroup',\n" +
//                "      'scan.startup.mode' = 'earliest-offset',\n" +
//                "      'format' = 'json',\n" +
//                "      'json.fail-on-missing-field' = 'false',\n" +
//                "      'json.ignore-parse-errors' = 'true'\n" +
//                ")\n" +
//                ";");
//
//        // 3 创建用户表
//        tableEnvironment.executeSql("create table tbl_user(\n" +
//                "    user_id           int            comment '用户ID',\n" +
//                "    user_name         string         comment '用户名称',\n" +
//                "    sex               int            comment '性别',\n" +
//                "    account           string         comment '账号',\n" +
//                "    nick_name         string         comment '昵称',\n" +
//                "    city_id           int            comment '城市ID',\n" +
//                "    city_name         string         comment '城市名称',\n" +
//                "    crowd_type        string         comment '人群类型',\n" +
//                "    register_time     timestamp(3)   comment '注册时间',\n" +
//                "    primary key(user_id) not enforced, -- 定义主键约束，但不做强校验\n" +
//                "    watermark for register_time as register_time -- 通过watermark定义事件时间  \n" +
//                ") with (\n" +
//                "        'connector' = 'kafka',\n" +
//                "        'properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "        'topic' = 'user_topic',\n" +
//                "        'properties.group.id' = 'testGroup',\n" +
//                "        'scan.startup.mode' = 'earliest-offset',\n" +
//                "        'format' = 'json',\n" +
//                "        'json.fail-on-missing-field' = 'false',\n" +
//                "        'json.ignore-parse-errors' = 'true'\n" +
//                ")\n" +
//                ";");
//
//        // 4 创建订单表
//        tableEnvironment.executeSql("create table tbl_order(\n" +
//                "    order_id            int             comment '订单ID',\n" +
//                "    shop_id             int             comment '书店ID',\n" +
//                "    user_id             int             comment '用户ID',\n" +
//                "    original_price      double          comment '原始交易额',\n" +
//                "    actual_price        double          comment '实付交易额',\n" +
//                "    discount_price      double          comment '折扣金额',\n" +
//                "    create_time         timestamp(3)    comment '下单时间',\n" +
//                "    -- 声明 create_time 是事件时间属性，并且用 延迟 5 秒的策略来生成 watermark\n" +
//                "    watermark for create_time as create_time - interval '5' second\n" +
//                ") with (\n" +
//                "    'connector' = 'kafka',\n" +
//                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "    'topic' = 'order_topic',\n" +
//                "    'properties.group.id' = 'testGroup',\n" +
//                "    'scan.startup.mode' = 'earliest-offset',\n" +
//                "    'format' = 'json',\n" +
//                "    'json.fail-on-missing-field' = 'false',\n" +
//                "    'json.ignore-parse-errors' = 'true'\n" +
//                ")\n" +
//                ";\n");
//
//        // 5 创建订单详情表
//        tableEnvironment.executeSql("create table tbl_order_detail(\n" +
//                "    order_book_id           int            comment '订单明细ID',\n" +
//                "    order_id                int            comment '订单ID',\n" +
//                "    book_id                 int            comment '图书ID',\n" +
//                "    book_number             int            comment '图书下单数量',\n" +
//                "    original_price          double         comment '原始交易额',\n" +
//                "    actual_price            double         comment '实付交易额',\n" +
//                "    discount_price          double         comment '折扣金额',\n" +
//                "    create_time             timestamp(3)   comment '下单时间',\n" +
//                "    -- 声明 create_time 是事件时间属性，并且用 延迟 5 秒的策略来生成 watermark\n" +
//                "    watermark for create_time as create_time - interval '5' second\n" +
//                ") with (\n" +
//                "     'connector' = 'kafka',\n" +
//                "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "     'topic' = 'order_book_topic',\n" +
//                "     'properties.group.id' = 'testGroup',\n" +
//                "     'scan.startup.mode' = 'earliest-offset',\n" +
//                "     'format' = 'json',\n" +
//                "     'json.fail-on-missing-field' = 'false',\n" +
//                "     'json.ignore-parse-errors' = 'true'\n" +
//                ")\n" +
//                ";\n");
//
//
//        // 6 创建jdbc聚合表
//        tableEnvironment.executeSql("create table tbl_shop_indi(\n" +
//                "    create_hour         string    comment '数据时间-小时',\n" +
//                "    shop_id             int       comment '书店ID',\n" +
//                "    original_amt        double    comment '总原始交易额',\n" +
//                "    actual_amt          double    comment '总实付交易额',\n" +
//                "    discount_amt        double    comment '总折扣金额',\n" +
//                "    order_cnt           bigint    comment '订单数',\n" +
//                "    PRIMARY KEY (create_hour,shop_id) NOT ENFORCED\n" +
//                ") with (\n" +
//                "   'connector' = 'jdbc',\n" +
//                "   'url' = 'jdbc:mysql://localhost:3306/chavin',\n" +
//                "   'driver' = 'com.mysql.jdbc.Driver',\n" +
//                "   'username' = 'root',\n" +
//                "   'password' = 'mysql',\n" +
//                "   'table-name' = 'tbl_shop_indi',\n" +
//                "   'sink.buffer-flush.max-rows' = '100',\n" +
//                "   'sink.buffer-flush.interval' = '3s',\n" +
//                "   'sink.max-retries' = '3'\n" +
//                ")\n" +
//                ";\n");


        // 测试
        tableEnvironment.executeSql("select * from tbl_book_shop").print();


    }
}
