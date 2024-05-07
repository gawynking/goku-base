package com.gawyn.sql;

import com.gawyn.function.DimProductWithVersionsForRedis;
import com.gawyn.function.DimProductWithVersionsForRedisUDTF;
import com.gawyn.function.FromUnixtimeScalarFunction;
import com.gawyn.function.PassThroughFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RedisDimQueryDemo {


    public static void main(String[] args) {

//        redisUDFDimQueryDemo();

//        redisUDFDimQueryWithSubQueryDemo();

//        redisUDFDimQueryWithSubQueryPTDemo();

        redisUDFDimQueryWithSubQueryUDTFDemo();
    }



    public static void redisUDFDimQueryDemo(){

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.createTemporarySystemFunction("dim_product_with_versions", DimProductWithVersionsForRedis.class);


        tableEnvironment.executeSql("create table tbl_order_detail(\n" +
                "    order_book_id           int            comment '订单明细ID',\n" +
                "    order_id                int            comment '订单ID',\n" +
                "    book_id                 int            comment '图书ID',\n" +
                "    book_number             int            comment '图书下单数量',\n" +
                "    original_price          double         comment '原始交易额',\n" +
                "    actual_price            double         comment '实付交易额',\n" +
                "    discount_price          double         comment '折扣金额',\n" +
                "    create_time             string         comment '下单时间',\n" +
                "    update_time             bigint         comment '更新时间戳'\n" +
                ")with(\n" +
                "     'connector' = 'kafka',\n" +
                "     'topic' = 'tbl_order_detail',\n" +
                "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "     'properties.group.id' = 'testGroup',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " );");


        tableEnvironment.executeSql("select\n" +
                "\n" +
                "    order_book_id                                                                                                                as     order_book_id,\n" +
                "    order_id                                                                                                                     as     order_id,\n" +
                "    book_id                                                                                                                      as     book_id,\n" +
                "    book_number                                                                                                                  as     book_number,\n" +
                "    original_price                                                                                                               as     original_price,\n" +
                "    actual_price                                                                                                                 as     actual_price,\n" +
                "    discount_price                                                                                                               as     discount_price,\n" +
                "    create_time                                                                                                                  as     create_time,\n" +
                "    update_time                                                                                                                  as     update_time,\n" +
                "    json_value(dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)),'$.price')     as book_price\n" +
                "\n" +
                "from tbl_order_detail t1\n" +
                ";").print();

    }


    public static void redisUDFDimQueryWithSubQueryDemo(){

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        // 注册UDF
        tableEnvironment.createTemporarySystemFunction("dim_product_with_versions", DimProductWithVersionsForRedis.class);

        // 定义订单明细数据源
        tableEnvironment.executeSql("create table tbl_order_detail(\n" +
                "    order_book_id           int            comment '订单明细ID',\n" +
                "    order_id                int            comment '订单ID',\n" +
                "    book_id                 int            comment '图书ID',\n" +
                "    book_number             int            comment '图书下单数量',\n" +
                "    original_price          double         comment '原始交易额',\n" +
                "    actual_price            double         comment '实付交易额',\n" +
                "    discount_price          double         comment '折扣金额',\n" +
                "    create_time             string         comment '下单时间',\n" +
                "    update_time             bigint         comment '更新时间戳'\n" +
                ")with(\n" +
                "     'connector' = 'kafka',\n" +
                "     'topic' = 'tbl_order_detail',\n" +
                "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "     'properties.group.id' = 'testGroup',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " );");

        String explainSql = tableEnvironment.explainSql("select\n" +
                "\n" +
                "    order_book_id,\n" +
                "    order_id,\n" +
                "    book_id,\n" +
                "    json_value(dim_product,'$.price') as price,\n" +
                "    json_value(dim_product,'$.book_name') as book_name\n" +
                "\n" +
                "from (\n" +
                "    select\n" +
                "\n" +
                "        order_book_id                                                                                      as order_book_id,\n" +
                "        order_id                                                                                           as order_id,\n" +
                "        book_id                                                                                            as book_id,\n" +
                "        book_number                                                                                        as book_number,\n" +
                "        original_price                                                                                     as original_price,\n" +
                "        actual_price                                                                                       as actual_price,\n" +
                "        discount_price                                                                                     as discount_price,\n" +
                "        create_time                                                                                        as create_time,\n" +
                "        update_time                                                                                        as update_time,\n" +
                "        dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)) as dim_product\n" +
                "\n" +
                "    from tbl_order_detail\n" +
                ") tmp\n" +
                "where json_value(dim_product,'$.price') > 5\n" +
                ";");

        System.out.println("SQL Explain Plan: ");
        System.out.println(explainSql);


        // 定义ETL逻辑，支持订单明细数据源每一条数据查询Redis维表获取对应的维度信息
        tableEnvironment.executeSql("select\n" +
                "\n" +
                "    order_book_id,\n" +
                "    order_id,\n" +
                "    book_id,\n" +
                "    json_value(dim_product,'$.price') as price,\n" +
                "    json_value(dim_product,'$.book_name') as book_name\n" +
                "\n" +
                "from (\n" +
                "    select\n" +
                "\n" +
                "        order_book_id                                                                                      as order_book_id,\n" +
                "        order_id                                                                                           as order_id,\n" +
                "        book_id                                                                                            as book_id,\n" +
                "        book_number                                                                                        as book_number,\n" +
                "        original_price                                                                                     as original_price,\n" +
                "        actual_price                                                                                       as actual_price,\n" +
                "        discount_price                                                                                     as discount_price,\n" +
                "        create_time                                                                                        as create_time,\n" +
                "        update_time                                                                                        as update_time,\n" +
                "        dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)) as dim_product\n" +
                "\n" +
                "    from tbl_order_detail\n" +
                ") tmp\n" +
                "where json_value(dim_product,'$.price') > 5\n" +
                ";").print();
    }

    public static void redisUDFDimQueryWithSubQueryPTDemo(){

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        // 注册UDF
        tableEnvironment.createTemporarySystemFunction("dim_product_with_versions", DimProductWithVersionsForRedis.class);
        tableEnvironment.createTemporarySystemFunction("pass_through", PassThroughFunction.class);

        // 定义订单明细数据源
        tableEnvironment.executeSql("create table tbl_order_detail(\n" +
                "    order_book_id           int            comment '订单明细ID',\n" +
                "    order_id                int            comment '订单ID',\n" +
                "    book_id                 int            comment '图书ID',\n" +
                "    book_number             int            comment '图书下单数量',\n" +
                "    original_price          double         comment '原始交易额',\n" +
                "    actual_price            double         comment '实付交易额',\n" +
                "    discount_price          double         comment '折扣金额',\n" +
                "    create_time             string         comment '下单时间',\n" +
                "    update_time             bigint         comment '更新时间戳'\n" +
                ")with(\n" +
                "     'connector' = 'kafka',\n" +
                "     'topic' = 'tbl_order_detail',\n" +
                "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "     'properties.group.id' = 'testGroup',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " );");

        // 打印执行计划
        String explainSql = tableEnvironment.explainSql("select\n" +
                "\n" +
                "    order_book_id,\n" +
                "    order_id,\n" +
                "    book_id,\n" +
                "    json_value(product,'$.price')     as price,\n" +
                "    json_value(product,'$.book_name') as book_name\n" +
                "\n" +
                "from (\n" +
                "    select\n" +
                "\n" +
                "        order_book_id   as order_book_id,\n" +
                "        order_id        as order_id,\n" +
                "        book_id         as book_id,\n" +
                "        book_number     as book_number,\n" +
                "        original_price  as original_price,\n" +
                "        actual_price    as actual_price,\n" +
                "        discount_price  as discount_price,\n" +
                "        create_time     as create_time,\n" +
                "        update_time     as update_time,\n" +
                "        product         as product \n" +
                "\n" +
                "    from tbl_order_detail\n" +
                "    left join lateral table(pass_through(dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)))) t(product) on true \n" +
                ") tmp\n" +
                "where json_value(product,'$.price') > 5\n" +
                ";");

        System.out.println("SQL Explain Plan: ");
        System.out.println(explainSql);


        // 定义ETL逻辑，支持订单明细数据源每一条数据查询Redis维表获取对应的维度信息
        tableEnvironment.executeSql("select\n" +
                "\n" +
                "    order_book_id,\n" +
                "    order_id,\n" +
                "    book_id,\n" +
                "    json_value(product,'$.price')     as price,\n" +
                "    json_value(product,'$.book_name') as book_name\n" +
                "\n" +
                "from (\n" +
                "    select\n" +
                "\n" +
                "        order_book_id   as order_book_id,\n" +
                "        order_id        as order_id,\n" +
                "        book_id         as book_id,\n" +
                "        book_number     as book_number,\n" +
                "        original_price  as original_price,\n" +
                "        actual_price    as actual_price,\n" +
                "        discount_price  as discount_price,\n" +
                "        create_time     as create_time,\n" +
                "        update_time     as update_time,\n" +
                "        product         as product \n" +
                "\n" +
                "    from tbl_order_detail\n" +
                "    left join lateral table(pass_through(dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)))) t(product) on true \n" +
                ") tmp\n" +
                "where json_value(product,'$.price') > 5\n" +
                ";").print();
    }

    public static void redisUDFDimQueryWithSubQueryUDTFDemo(){

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        // 注册UDF
        tableEnvironment.createTemporarySystemFunction("dim_product_with_versions", DimProductWithVersionsForRedisUDTF.class);

        // 定义订单明细数据源
        tableEnvironment.executeSql("create table tbl_order_detail(\n" +
                "    order_book_id           int            comment '订单明细ID',\n" +
                "    order_id                int            comment '订单ID',\n" +
                "    book_id                 int            comment '图书ID',\n" +
                "    book_number             int            comment '图书下单数量',\n" +
                "    original_price          double         comment '原始交易额',\n" +
                "    actual_price            double         comment '实付交易额',\n" +
                "    discount_price          double         comment '折扣金额',\n" +
                "    create_time             string         comment '下单时间',\n" +
                "    update_time             bigint         comment '更新时间戳'\n" +
                ")with(\n" +
                "     'connector' = 'kafka',\n" +
                "     'topic' = 'tbl_order_detail',\n" +
                "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "     'properties.group.id' = 'testGroup',\n" +
                "     'scan.startup.mode' = 'latest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " );");

        // 打印执行计划
        String explainSql = tableEnvironment.explainSql("select\n" +
                "\n" +
                "    order_book_id,\n" +
                "    order_id,\n" +
                "    book_id,\n" +
                "    json_value(product,'$.price')     as price,\n" +
                "    json_value(product,'$.book_name') as book_name\n" +
                "\n" +
                "from (\n" +
                "    select\n" +
                "\n" +
                "        order_book_id   as order_book_id,\n" +
                "        order_id        as order_id,\n" +
                "        book_id         as book_id,\n" +
                "        book_number     as book_number,\n" +
                "        original_price  as original_price,\n" +
                "        actual_price    as actual_price,\n" +
                "        discount_price  as discount_price,\n" +
                "        create_time     as create_time,\n" +
                "        update_time     as update_time,\n" +
                "        product         as product \n" +
                "\n" +
                "    from tbl_order_detail\n" +
                "    left join lateral table(dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint))) t(product) on true \n" +
                ") tmp\n" +
                "where json_value(product,'$.price') > 5\n" +
                ";");

        System.out.println("SQL Explain Plan: ");
        System.out.println(explainSql);


        // 定义ETL逻辑，支持订单明细数据源每一条数据查询Redis维表获取对应的维度信息
        tableEnvironment.executeSql("select\n" +
                "\n" +
                "    order_book_id,\n" +
                "    order_id,\n" +
                "    book_id,\n" +
                "    json_value(product,'$.price')     as price,\n" +
                "    json_value(product,'$.book_name') as book_name\n" +
                "\n" +
                "from (\n" +
                "    select\n" +
                "\n" +
                "        order_book_id   as order_book_id,\n" +
                "        order_id        as order_id,\n" +
                "        book_id         as book_id,\n" +
                "        book_number     as book_number,\n" +
                "        original_price  as original_price,\n" +
                "        actual_price    as actual_price,\n" +
                "        discount_price  as discount_price,\n" +
                "        create_time     as create_time,\n" +
                "        update_time     as update_time,\n" +
                "        product         as product \n" +
                "\n" +
                "    from tbl_order_detail\n" +
                "    left join lateral table(dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint))) t(product) on true \n" +
                ") tmp\n" +
                "where json_value(product,'$.price') > 5\n" +
                ";").print();
    }

}
