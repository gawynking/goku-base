package com.pgman.goku.table;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * flink-1.10.0 版本 构建flink Table执行环境方法
 *
 */
public class CreateTableEnvironmentExample {

    public static void main(String[] args) {

    }

    /**
     * 1 流 ： 创建flink流table执行环境
     */
    public static void createFlinkStreamTableEnvironment(){

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
        // or TableEnvironment fsTableEnv = TableEnvironment.create(fsSettings);



    }


    /**
     * 2 批 ： 创建flink批table执行环境
     */
    public static void createFlinkBatchTableEnvironment(){

        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);


    }


    /**
     * 3 流 ： 创建blink流table执行环境
     */
    public static void createBlinkStreamTableEnvironment(){

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        // or TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);




    }

    /**
     * 4 批 ： 创建Blink批table执行环境
     */
    public static void createBlinkBatchTableEnvironment(){

        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);


    }


}
