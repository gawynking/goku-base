package com.gawyn.sql;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PreserveOrderSQLDemo {

    public static void main(String[] args) {

//        preserveOrderAggregate();

        preserveOrderJoin();

    }


    public static void preserveOrderAggregate(){
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.executeSql("create table tbl_order_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '0' second\n" +
                ")with(\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'tbl_order_source',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");

        tableEnvironment.executeSql("create table ods_order_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '订单金额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '0' second,\n" +
                "    primary key (order_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'ods_order_source',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("insert into ods_order_source\n" +
                "select\n" +
                "\ttmp.order_id,\n" +
                "\ttmp.shop_id,\n" +
                "\ttmp.user_id,\n" +
                "\ttmp.original_price,\n" +
                "\ttmp.create_time\n" +
                "from (\n" +
                "\tselect\n" +
                "\t    t.order_id,\n" +
                "\t    t.shop_id,\n" +
                "\t    t.user_id,\n" +
                "\t    t.original_price,\n" +
                "\t    t.create_time,\n" +
                "\t\trow_number()over(partition by t.order_id order by t.create_time asc) as rn\n" +
                "\tfrom tbl_order_source t\n" +
                ") tmp\n" +
                "where tmp.rn = 1\n" +
                ";");

        // 测试1：
//        tableEnvironment.executeSql("select * from ods_order_source").print();
        /* 对应的输出
            +----+-------------+-------------+-------------+--------------------------------+-------------------------+
            | op |    order_id |     shop_id |     user_id |                 original_price |             create_time |
            +----+-------------+-------------+-------------+--------------------------------+-------------------------+
            | +I |           1 |           1 |           1 |                            1.0 | 2024-01-01 20:05:00.000 |
            | +I |           2 |           1 |           2 |                            2.0 | 2024-01-01 20:04:00.000 |
            | -U |           1 |           1 |           1 |                            1.0 | 2024-01-01 20:05:00.000 |
            | +U |           1 |           1 |           1 |                            3.0 | 2024-01-01 20:03:00.000 |
            | +I |           3 |           1 |           3 |                            4.0 | 2024-01-01 20:02:00.000 |
         */

        // 测试2：
//        tableEnvironment.executeSql("select\n" +
//                "    t.shop_id                                  as shop_id,\n" +
//                "    to_date(cast(t.create_time as string))     as create_date,\n" +
//                "    sum(t.original_price)                      as original_amt,\n" +
//                "    sum(1)                                     as order_num,\n" +
//                "    count(distinct t.order_id)                 as order_cnt\n" +
//                "from ods_order_source t\n" +
//                "group by\n" +
//                "    t.shop_id,\n" +
//                "    to_date(cast(t.create_time as string))\n" +
//                ";").print();
        /* 测试输出
            +----+-------------+-------------+--------------------------------+-------------+----------------------+
            | op |     shop_id | create_date |                   original_amt |   order_num |            order_cnt |
            +----+-------------+-------------+--------------------------------+-------------+----------------------+
            {"order_id":"1","shop_id":"1","user_id":"1","original_price":"1","create_time":"2024-01-01 20:05:00"}
            | +I |           1 |  2024-01-01 |                            1.0 |           1 |                    1 |
            {"order_id":"2","shop_id":"1","user_id":"2","original_price":"2","create_time":"2024-01-01 20:04:00"}
            | -U |           1 |  2024-01-01 |                            1.0 |           1 |                    1 |
            | +U |           1 |  2024-01-01 |                            3.0 |           2 |                    2 |
            {"order_id":"1","shop_id":"1","user_id":"1","original_price":"3","create_time":"2024-01-01 20:03:00"}
            | -U |           1 |  2024-01-01 |                            3.0 |           2 |                    2 |
            | +U |           1 |  2024-01-01 |                            2.0 |           1 |                    1 |
            | -U |           1 |  2024-01-01 |                            2.0 |           1 |                    1 |
            | +U |           1 |  2024-01-01 |                            5.0 |           2 |                    2 |
            {"order_id":"3","shop_id":"1","user_id":"3","original_price":"4","create_time":"2024-01-01 20:02:00"}
            | -U |           1 |  2024-01-01 |                            5.0 |           2 |                    2 |
            | +U |           1 |  2024-01-01 |                            9.0 |           3 |                    3 |
            {"order_id":"1","shop_id":"1","user_id":"1","original_price":"5","create_time":"2024-01-01 20:04:00"}
         */

        // 测试3：
        tableEnvironment.executeSql("select\n" +
                "    t.shop_id                                  as shop_id,\n" +
                "    to_date(cast(t.create_time as string))     as create_date,\n" +
                "    sum(t.original_price)                      as original_amt,\n" +
                "    sum(1)                                     as order_num,\n" +
                "    count(distinct t.order_id)                 as order_cnt\n" +
                "from tbl_order_source t\n" +
                "group by\n" +
                "    t.shop_id,\n" +
                "    to_date(cast(t.create_time as string))\n" +
                ";").print();
        /* 输出结果
            +----+-------------+-------------+--------------------------------+-------------+----------------------+
            | op |     shop_id | create_date |                   original_amt |   order_num |            order_cnt |
            +----+-------------+-------------+--------------------------------+-------------+----------------------+
            | +I |           1 |  2024-01-01 |                            1.0 |           1 |                    1 |
            | -U |           1 |  2024-01-01 |                            1.0 |           1 |                    1 |
            | +U |           1 |  2024-01-01 |                            3.0 |           2 |                    2 |
            | -U |           1 |  2024-01-01 |                            3.0 |           2 |                    2 |
            | +U |           1 |  2024-01-01 |                            6.0 |           3 |                    2 |
            | -U |           1 |  2024-01-01 |                            6.0 |           3 |                    2 |
            | +U |           1 |  2024-01-01 |                           10.0 |           4 |                    3 |
            | -U |           1 |  2024-01-01 |                           10.0 |           4 |                    3 |
            | +U |           1 |  2024-01-01 |                           15.0 |           5 |                    3 |
         */

    }


    public static void preserveOrderJoin(){
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.executeSql("create table tbl_order_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '原始交易额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '0' second\n" +
                ")with(\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'tbl_order_source',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_order_payment_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    payment_amount      double          comment '支付金额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '0' second\n" +
                ")with(\n" +
                "     'connector' = 'kafka',\n" +
                "     'topic' = 'tbl_order_payment_source',\n" +
                "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "     'properties.group.id' = 'testGroup',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " );");

        tableEnvironment.executeSql("create table ods_order_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    original_price      double          comment '订单金额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '0' second,\n" +
                "    primary key (order_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'ods_order_source',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("create table ods_order_payment_source(\n" +
                "    order_id            int             comment '订单ID',\n" +
                "    payment_amount      double          comment '支付金额',\n" +
                "    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',\n" +
                "    watermark for create_time as create_time - interval '0' second,\n" +
                "    primary key (order_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'ods_order_payment_source',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("insert into ods_order_source\n" +
                "select\n" +
                "\ttmp.order_id,\n" +
                "\ttmp.shop_id,\n" +
                "\ttmp.user_id,\n" +
                "\ttmp.original_price,\n" +
                "\ttmp.create_time\n" +
                "from (\n" +
                "\tselect\n" +
                "\t    t.order_id,\n" +
                "\t    t.shop_id,\n" +
                "\t    t.user_id,\n" +
                "\t    t.original_price,\n" +
                "\t    t.create_time,\n" +
                "\t\trow_number()over(partition by t.order_id order by t.create_time asc) as rn\n" +
                "\tfrom tbl_order_source t\n" +
                ") tmp\n" +
                "where tmp.rn = 1\n" +
                ";\n");

        tableEnvironment.executeSql("insert into ods_order_payment_source\n" +
                "select\n" +
                "\ttmp.order_id,\n" +
                "\ttmp.payment_amount,\n" +
                "\ttmp.create_time\n" +
                "from (\n" +
                "\tselect\n" +
                "\t    t.order_id,\n" +
                "\t    t.payment_amount,\n" +
                "\t    t.create_time,\n" +
                "\t\trow_number()over(partition by t.order_id order by t.create_time asc) as rn\n" +
                "\tfrom tbl_order_payment_source t\n" +
                ") tmp\n" +
                "where tmp.rn = 1\n" +
                ";\n");


        tableEnvironment.executeSql("select \n" +
                "\n" +
                "    t1.order_id,\n" +
                "    t1.shop_id,\n" +
                "    t1.user_id,\n" +
                "    t1.original_price,\n" +
                "    t2.payment_amount \n" +
                "\n" +
                "from tbl_order_source t1 \n" +
                "left join tbl_order_payment_source t2 \n" +
                "     on t1.order_id = t2.order_id \n" +
                ";\n").print();

    }

}
