package com.gawyn.sql;

import com.gawyn.function.OrderTagTableFunction;
import com.gawyn.function.RedisBitmapUVFunction;
import com.gawyn.function.RedisSetUVFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RedisUVDemo {


    public static void main(String[] args) {
//        redisSetUVDemo();
//        redisHLLUVDemo();
//        redisBitmapUVDemo();
//        redisSetUVDemoFromUpsertKafka();
//        redisHLLUVDemoFromUpsertKafka();
        redisBitmapUVDemoFromUpsertKafka();
    }


    public static void redisSetUVDemo(){
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.createTemporarySystemFunction("redis_uv_set", RedisSetUVFunction.class);

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
                "    'topic' = 'tbl_order_source',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");


        tableEnvironment.executeSql("select \n" +
                "    shop_id,\n" +
                "    redis_uv_set(concat('gawyn:',cast(shop_id as string)),cast(user_id as string)) as shop_user_cnt\n" +
                "from tbl_order_source \n" +
                "group by \n" +
                "    shop_id\n" +
                ";").print();

    }

    public static void redisSetUVDemoFromUpsertKafka(){
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.createTemporarySystemFunction("redis_uv_set", RedisSetUVFunction.class);

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
                "    'topic' = 'tbl_order_source',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_order_shop_user(\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    primary key(user_id,shop_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'tbl_order_shop_user',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");\n");

        tableEnvironment.executeSql("insert into tbl_order_shop_user \n" +
                "select \n" +
                "    user_id,\n" +
                "    shop_id\n" +
                "from tbl_order_source \n" +
                "group by \n" +
                "    user_id,\n" +
                "    shop_id\n" +
                ";");

        tableEnvironment.executeSql("select \n" +
                "    shop_id,\n" +
                "    redis_uv_set(concat('gawyn:',cast(shop_id as string)),cast(user_id as string)) as shop_user_cnt\n" +
                "from tbl_order_shop_user \n" +
                "group by \n" +
                "    shop_id\n" +
                ";").print();

    }

    public static void redisHLLUVDemo(){
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.createTemporarySystemFunction("redis_uv_hll", RedisSetUVFunction.class);

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
                "    'topic' = 'tbl_order_source',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");


        tableEnvironment.executeSql("select \n" +
                "    shop_id,\n" +
                "    redis_uv_hll(concat('gawyn:',cast(shop_id as string)),cast(user_id as string)) as shop_user_cnt\n" +
                "from tbl_order_source \n" +
                "group by \n" +
                "    shop_id\n" +
                ";").print();

    }

    public static void redisHLLUVDemoFromUpsertKafka(){
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.createTemporarySystemFunction("redis_uv_hll", RedisSetUVFunction.class);

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
                "    'topic' = 'tbl_order_source',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_order_shop_user(\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    original_price      double          comment '',\n" +
                "    primary key(user_id,shop_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'tbl_order_shop_user',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("insert into tbl_order_shop_user\n" +
                "select\n" +
                "    user_id,\n" +
                "    shop_id,\n" +
                "    sum(original_price) as original_price \n" +
                "from tbl_order_source\n" +
                "group by\n" +
                "    user_id,\n" +
                "    shop_id\n" +
                ";");

        tableEnvironment.executeSql("select \n" +
                "    shop_id,\n" +
                "    redis_uv_hll(concat('gawyn1:',cast(shop_id as string)),cast(user_id as string)) as shop_user_cnt\n" +
                "from tbl_order_shop_user \n" +
                "group by \n" +
                "    shop_id\n" +
                ";").print();

    }

    public static void redisBitmapUVDemo(){
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.createTemporarySystemFunction("redis_uv_bitmap", RedisBitmapUVFunction.class);

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
                "    'topic' = 'tbl_order_source',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");


        tableEnvironment.executeSql("select \n" +
                "    shop_id,\n" +
                "    redis_uv_bitmap(concat('gawyn:shop_id:',cast(shop_id as string)),user_id) as shop_user_cnt\n" +
                "from tbl_order_source \n" +
                "group by \n" +
                "    shop_id\n" +
                ";").print();

    }

    public static void redisBitmapUVDemoFromUpsertKafka(){
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.createTemporarySystemFunction("redis_uv_bitmap", RedisBitmapUVFunction.class);

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
                "    'topic' = 'tbl_order_source',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ");");

        tableEnvironment.executeSql("create table tbl_order_shop_user(\n" +
                "    user_id             int             comment '用户ID',\n" +
                "    shop_id             int             comment '书店ID',\n" +
                "    original_price      double          comment '',\n" +
                "    primary key(user_id,shop_id) not enforced\n" +
                ")with(\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'tbl_order_shop_user',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ");");

        tableEnvironment.executeSql("insert into tbl_order_shop_user\n" +
                "select\n" +
                "    user_id,\n" +
                "    shop_id,\n" +
                "    sum(original_price) as original_price \n" +
                "from tbl_order_source\n" +
                "group by\n" +
                "    user_id,\n" +
                "    shop_id\n" +
                ";");

        tableEnvironment.executeSql("select \n" +
                "    shop_id,\n" +
                "    redis_uv_bitmap(concat('gawyn2:shop_id:',cast(shop_id as string)),user_id) as shop_user_cnt\n" +
                "from tbl_order_source \n" +
                "group by \n" +
                "    shop_id\n" +
                ";").print();

    }

}
