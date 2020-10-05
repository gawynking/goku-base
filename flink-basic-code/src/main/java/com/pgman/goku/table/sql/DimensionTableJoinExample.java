package com.pgman.goku.table.sql;

import com.pgman.goku.table.udf.ScalarGetJSONItem;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class DimensionTableJoinExample {


    public static void main(String[] args) {

        temporalTableFunctionExample();

    }


    /**
     * 维度表函数join示例
     *
     * 测试数据 ：
     *
     * {"currency":"Euro","order_amt":248,"id":1,"customer_id":1}
     * {"currency":"Yen","order_amt":248,"id":1,"customer_id":2}
     * {"currency":"US Dollar","order_amt":248,"id":1,"customer_id":2}
     */
    public static void temporalTableFunctionExample(){

        // 1.1 创建Flink执行环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // 1.2 启用检查点功能
        bsEnv.enableCheckpointing(1000);
        bsEnv.setStateBackend(new FsStateBackend("hdfs://cdh01:8020/flink/ck02"));
        bsEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        bsEnv.getCheckpointConfig().setCheckpointTimeout(5000);
        bsEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        bsEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 取消任务不清楚检查点信息
        bsEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,100));


        bsTableEnv.registerFunction("get_json_object",new ScalarGetJSONItem());

        // 定义维度表
        List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
        ratesHistoryData.add(Tuple2.of("Euro", 114L));
        ratesHistoryData.add(Tuple2.of("Yen", 1L));
        ratesHistoryData.add(Tuple2.of("Euro", 116L));
        ratesHistoryData.add(Tuple2.of("Euro", 119L));
        ratesHistoryData.add(Tuple2.of("Euro", 120L));


        DataStream<Tuple2<String, Long>> ratesHistoryStream = bsEnv.fromCollection(ratesHistoryData);
        Table ratesHistory = bsTableEnv.fromDataStream(ratesHistoryStream, "currency, rate, proctime.proctime");

        TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("proctime", "currency"); // INNER JOIN
        bsTableEnv.registerFunction("Rates", rates);

        // 定义Orders表
        String orderSQL = "create table Orders(\n" +
                "\n" +
                "json string comment '事件日志',\n" +
                "proc_time as proctime()" +
//                ",WATERMARK FOR proc_time AS proc_time - INTERVAL '5' SECOND" +
                "\n" +
                ") with (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = '0.11',\n" +
                "'connector.topic' = 'order_topic',\n" +
                "'connector.properties.zookeeper.connect' = 'test.server:2181',\n" +
                "'connector.properties.bootstrap.servers' = 'test.server:9092',\n" +
                "'connector.properties.group.id' = 'table-source-kafka-group100060',\n" +
                "'connector.startup-mode' = 'group-offsets',\n" +
                "\n" +
                "'format.type' = 'csv',\n" +
                "'format.field-delimiter' = '|'\n" +
                ")";

        bsTableEnv.sqlUpdate(orderSQL);

        // 维表 inner join sql ,目前测试不支持 left outer join
        String innerJoinSQL = "SELECT \n" +
                "\n" +
                "  get_json_object(o.json,'$.customer_id') as customer_id,\n" +
                "  get_json_object(o.json,'$.currency') as currency,\n" +
                "  r.rate \n" +
                "  \n" +
                "FROM Orders AS o,\n" +
                "  LATERAL TABLE (Rates(o.proc_time)) AS r\n" +
                "WHERE r.currency =  get_json_object(o.json,'$.currency')";

        Table table = bsTableEnv.sqlQuery(innerJoinSQL);
        bsTableEnv.toAppendStream(table, Row.class).print();


        try {
            bsTableEnv.execute("SQL Join");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }




}
