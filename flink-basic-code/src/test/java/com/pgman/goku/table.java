package com.pgman.goku;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class table {


    public static void main(String[] args) {

        test();

    }


    public static void test() {

        // 1.1 创建Flink执行环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);


        String tblOrderSQL = "create table yw_order(\n" +
                "\n" +
                "json string \n" +
                "\n" +
                ") with (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = '0.11',\n" +
                "'connector.startup-mode' = 'group-offsets',\n" +
                "'connector.topic' = 'pgman',\n" +
                "'connector.properties.zookeeper.connect' = 'test.server:2181',\n" +
                "'connector.properties.bootstrap.servers' = 'test.server:9092',\n" +
                "\n" +
                "'format.type' = 'csv',\n" +
                "'format.field-delimiter' = '\t'" +
                ")";

        bsTableEnv.sqlUpdate(tblOrderSQL);

        Table table = bsTableEnv.sqlQuery("select unix_timestamp()*1000 from yw_order");

        bsTableEnv.toRetractStream(table,Row.class).print();


        try {
            bsTableEnv.execute("Flink SQL");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }



}
