package com.pgman.goku.table.sql;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Flink SQL示例类
 *
 *
 */
public class SQLExample {

    public static void main(String[] args) {

        String groupId = "sql-example";
        parseBinlog();


    }


    public static void parseBinlog() {

        // 1.1 创建Flink执行环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv);

        String tblOrderSQL = "create table yw_order(\n" +
                "\n" +
                "json string \n" +
                "\n" +
                ") with (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = '0.11',\n" +
                "'connector.startup-mode' = 'group-offsets',\n" +
                "\n" +
                "-- 生产：\n" +
                "\n" +
                "-- 测试：\n" +
                "'connector.topic' = 'pgman',\n" +
                "'connector.properties.zookeeper.connect' = 'test.server:2181',\n" +
                "'connector.properties.bootstrap.servers' = 'test.server:9092',\n" +
                "\n" +
                "'format.type' = 'csv',\n" +
                "'format.field-delimiter' = '\t'" +
                "\n" +
                ")";

        bsTableEnv.executeSql(tblOrderSQL);

        Table table = bsTableEnv.sqlQuery("select \n" +
                "\n" +
                "get_json_object(substr(get_json_object(json,'$.data'),2,char_length(get_json_object(json,'$.data'))-2),'$.id')                                                                   as clue_id,\n" +
                "get_json_object(substr(get_json_object(json,'$.data'),2,char_length(get_json_object(json,'$.data'))-2),'$.user_id')                                                              as user_id,\n" +
                "get_json_object(substr(get_json_object(json,'$.data'),2,char_length(get_json_object(json,'$.data'))-2),'$.is_distribute')                                                        as is_distribute,\n" +
                "1                                                                                                                                                                                as from_source,\n" +
                "from_unixtime(cast(get_json_object(substr(get_json_object(json,'$.data'),2,char_length(get_json_object(json,'$.data'))-2),'$.create_datetime') as bigint),'yyyy-MM-dd HH:mm:ss') as create_time,\n" +
                "from_unixtime(cast(get_json_object(substr(get_json_object(json,'$.data'),2,char_length(get_json_object(json,'$.data'))-2),'$.update_datetime') as bigint),'yyyy-MM-dd HH:mm:ss') as update_time\n" +
                "\n" +
                "from yw_order \n" +
                "where get_json_object(json,'$.type') in ('INSERT','UPDATE') \n" +
                "and get_json_object(json,'$.isDdl') = 'false' \n" +
                "and get_json_object(json,'$.table') = 'yw_order' ");

        bsTableEnv.toRetractStream(table,Row.class).print();


        try {

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
