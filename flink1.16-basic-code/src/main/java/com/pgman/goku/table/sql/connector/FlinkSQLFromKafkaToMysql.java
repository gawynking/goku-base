package com.pgman.goku.table.sql.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class FlinkSQLFromKafkaToMysql {

    public static void main(String[] args) {
        fromKafkaToMysql();
    }


    public static void fromKafkaToMysql(){

        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT,8088);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .withConfiguration(configuration)
                .build();
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


        tableEnvironment.executeSql("create table tbl_shop_indi(\n" +
                "    create_hour         string    comment '数据时间-小时',\n" +
                "    shop_id             int       comment '书店ID',\n" +
                "    original_amt        double    comment '总原始交易额',\n" +
                "    actual_amt          double    comment '总实付交易额',\n" +
                "    discount_amt        double    comment '总折扣金额',\n" +
                "    order_cnt           bigint    comment '订单数',\n" +
                "    PRIMARY KEY (create_hour,shop_id) NOT ENFORCED\n" +
                ") with (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/chavin',\n" +
                "   'driver' = 'com.mysql.jdbc.Driver',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'mysql',\n" +
                "   'table-name' = 'tbl_shop_indi',\n" +
                "   'sink.buffer-flush.max-rows' = '100',\n" +
                "   'sink.buffer-flush.interval' = '3s',\n" +
                "   'sink.max-retries' = '3' \n" +
                ")");


        tableEnvironment.executeSql("insert into tbl_shop_indi\n" +
                "select\n" +
                "\n" +
                "    substr(create_time,1,13) as create_hour,\n" +
                "    shop_id                  as shop_id,\n" +
                "    sum(original_price)      as original_amt,\n" +
                "    sum(actual_price)        as actual_amt,\n" +
                "    sum(discount_price)      as discount_amt,\n" +
                "    count(distinct order_id)filter(where shop_id%2=0) as order_cnt\n" +
                "\n" +
                "from tbl_order\n" +
                "where shop_id <= 10\n" +
                "  and substr(create_time,1,13) >= '2023-06-04 12'\n" +
                "group by\n" +
                "    shop_id,\n" +
                "    substr(create_time,1,13)"
        );

    }
}
