package com.gawyn.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RedisSQLDemo {

    public static void main(String[] args) {

        test();
    }


    public static void test(){
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        executionEnvironment.setParallelism(1);
        tableEnvironment.getConfig().set("table.exec.source.idle-timeout","3s");

        tableEnvironment.executeSql("create table redis_test(\n" +
                "    rkey        string,\n" +
                "    value1      string\n" +
                ") with (\n" +
                "    'connector' = 'redis',\n" +
                "    'host' = '127.0.0.1',\n" +
                "    'port' = '6379',\n" +
                "    'redis-mode' = 'single',\n" +
                "    'key-column' = 'redis_test',\n" +
                "    'value-column' = 'value1',\n" +
                "    'lookup.hash.enable' = 'false',\n" +
                "    'lookup.redis.datatype' = 'list' \n" +
                ");");

        tableEnvironment.executeSql("select * from redis_test").print();



    }
}
