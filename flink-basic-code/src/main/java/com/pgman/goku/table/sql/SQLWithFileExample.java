package com.pgman.goku.table.sql;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.tool.FlinkSQLJDBCTools;
import com.pgman.goku.util.ObjectUtils;
import com.pgman.goku.util.SQLUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * 完全基于SQL实现流式计算示例
 *
 * 从检查点恢复数据：
 * flink run \
 * -m yarn-cluster \
 * -ynm test_cp \
 * -p 1 \
 * -c com.pgman.goku.table.sql.SQLWithFileExample \
 * -s hdfs://cdh01:8020/flink/ck02/772714cbd38114f35414f0b2f6104676/chk-288/_metadata \
 * /flink/test.jar /home/chavin/order.sql
 *
 */
public class SQLWithFileExample {

    private static final Logger logger = Logger.getLogger(SQLWithFileExample.class);


    public static void main(String[] args) {

        String sqlFile = null;
        if(args.length == 1){
            sqlFile = args[0];
        }

//        sqlStreamWithSQLFileExample(sqlFile);

        sqlStreamWithSQLFileExampleV2(sqlFile);

//        sqlStreamWithSubview(sqlFile);

    }



    /**
     * sql集成，支持建设中间视图
     *
     * @param sqlFile
     */
    public static void sqlStreamWithSubview(String sqlFile) {

        // 1 解析SQL文件 return {params,functions,tables,views,etlSQL}
        List[] sqlList = SQLUtils.parseSQLScript(sqlFile);


        // 2.1 创建Flink执行环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // 2.2 启用检查点功能
        bsEnv.enableCheckpointing(1000);
        if(SQLUtils.getCheckpointURL(sqlList[0]) != null) {
            bsEnv.setStateBackend(new FsStateBackend(SQLUtils.getCheckpointURL(sqlList[0])));
        } else {
            bsEnv.setStateBackend(new FsStateBackend("hdfs://cdh01:8020/flink/ck001"));
        }
        bsEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        bsEnv.getCheckpointConfig().setCheckpointTimeout(5000);
        bsEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        bsEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 取消任务不清楚检查点信息

        bsEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,100));


        // 3.1  声明时间语义
        if (SQLUtils.getTimeCharacteristic(sqlList[0]) == 2) {
            bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            System.out.println("程序以Event Time语义执行.");
            System.out.println();
        } else {
            bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
            System.out.println("程序以Processing Time语义执行.");
            System.out.println();
        }


        /**
         * 3.2 注册udf
         */
        for (Object sql : sqlList[1]) {

            String[] elements = SQLUtils.parseFunctionSQL(sql.toString());
            String functionName = elements[0];
            String functionClass = elements[1];

            try {
                Object object = Class.forName(functionClass).newInstance();
                if (object instanceof ScalarFunction) {
                    bsTableEnv.registerFunction(functionName, (ScalarFunction) object);
                } else if (object instanceof TableFunction) {
                    bsTableEnv.registerFunction(functionName, (TableFunction) object);
                } else if (object instanceof AggregateFunction) {
                    bsTableEnv.registerFunction(functionName, (AggregateFunction) object);
                } else if (object instanceof TableAggregateFunction) {
                    bsTableEnv.registerFunction(functionName, (TableAggregateFunction) object);
                } else {
                    logger.error("用户自定义语句不能创建函数 ： " + sql + "\n");
                    continue;
                }

                System.out.println("UDF : " + functionName + " 注册成功." + "\n");
            } catch (Exception e) {
                System.out.println("UDF : " + functionName + " 注册成功." + "\n");
                logger.error(e);
            }

        }


        /**
         * 3.3 注册table schema
         */
        for (Object sql : sqlList[2]) {

            bsTableEnv.sqlUpdate(sql.toString());
            System.out.println(sql.toString() + "\n");

        }


        /**
         * 3.4  注册view flink-1.10 暂不支持，需要额外封装
         */
        for (Object sql : sqlList[3]) {

            String[] viewInfo = SQLUtils.parseViewSQL(sql.toString());
            String viewName = viewInfo[0];
            String viewSQL = viewInfo[1];

            bsTableEnv.createTemporaryView(viewName,bsTableEnv.sqlQuery(viewSQL));
            System.out.println(sql.toString() + "\n");

        }


        /**
         * 3.5 注册etl逻辑
         */
        for (Object sql : sqlList[4]) {

            if (sql.toString().trim().toLowerCase().startsWith("insert")) {
                bsTableEnv.sqlUpdate(sql.toString());
                System.out.println(sql.toString() + "\n");
            }

        }


        // 4 启动Flink任务
        try {
            bsTableEnv.execute("Stream for SQL");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }






    /**
     * Flink基于sql etl增强版
     *
     * @param sqlFile
     */
    public static void sqlStreamWithSQLFileExampleV2(String sqlFile) {

        // 1 解析SQL文件
        List[] sqlList = SQLUtils.parseSQLScript(sqlFile);


        // 2 创建Flink执行环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);


        // 3 启用检查点功能
        bsEnv.enableCheckpointing(1000);
        bsEnv.setStateBackend(new FsStateBackend("hdfs://cdh01:8020/flink/ck02"));
        bsEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        bsEnv.getCheckpointConfig().setCheckpointTimeout(5000);
        bsEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        bsEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 取消任务不清楚检查点信息

        bsEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,100));

        bsEnv.setParallelism(1);



        // 4 声明时间语义
        if (SQLUtils.getTimeCharacteristic(sqlList[0]) == 2) {
            bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            System.out.println("程序以Event Time语义执行.");
            System.out.println();
        } else {
            bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
            System.out.println("程序以Processing Time语义执行.");
            System.out.println();
        }


        /**
         * 5.1 注册udf
         */
        for (Object sql : sqlList[1]) {

            String[] elements = SQLUtils.parseFunctionSQL(sql.toString());
            String functionName = elements[0];
            String functionClass = elements[1];

            try {
                Object object = Class.forName(functionClass).newInstance();
                if (object instanceof ScalarFunction) {
                    bsTableEnv.registerFunction(functionName, (ScalarFunction) object);
                } else if (object instanceof TableFunction) {
                    bsTableEnv.registerFunction(functionName, (TableFunction) object);
                } else if (object instanceof AggregateFunction) {
                    bsTableEnv.registerFunction(functionName, (AggregateFunction) object);
                } else if (object instanceof TableAggregateFunction) {
                    bsTableEnv.registerFunction(functionName, (TableAggregateFunction) object);
                } else {
                    logger.error("用户自定义语句不能创建函数 ： " + sql + "\n");
                    continue;
                }

                System.out.println("UDF : " + functionName + " 注册成功." + "\n");
            } catch (Exception e) {
                System.out.println("UDF : " + functionName + " 注册成功." + "\n");
                logger.error(e);
            }

        }


        /**
         * 5.2 注册table schema
         */
        for (Object sql : sqlList[2]) {

            bsTableEnv.sqlUpdate(sql.toString());
            System.out.println(sql.toString() + "\n");

        }


        /**
         * 5.3  注册view
         */
        for (Object sql : sqlList[3]) {

            String[] viewInfo = SQLUtils.parseViewSQL(sql.toString());
            String viewName = viewInfo[0];
            String viewSQL = viewInfo[1];

            bsTableEnv.createTemporaryView(viewName,bsTableEnv.sqlQuery(viewSQL));
            System.out.println(sql.toString() + "\n");

        }



        /**
         * 5.4 注册etl逻辑
         */
        for (Object sql : sqlList[4]) {

            if (sql.toString().trim().toLowerCase().startsWith("insert")) {

                if(SQLUtils.isUpsertMode(sql.toString())){


                    // 1 解析元数据信息
                    // 1.1 解析upsert语句
                    String[] upsertSQL = SQLUtils.parseUpsertSQL(sql.toString());
                    String tableName = upsertSQL[0];
                    String query = upsertSQL[1];

                    // 1.2 解析查询主键，用于更新的键
                    List<String> primaryKeys = SQLUtils.getPrimaryKeys(sql.toString());

                    // 1.3 通过目标表名称获取 建表语句
                    String sinkTableSQL = SQLUtils.getSinkCreateStatementFromTableName(tableName, sqlList[2]);

                    // 1.4 通过建表语句获得 jdbc连接信息
                    Map<String, String> jdbcInfo = SQLUtils.getJDBCInfo(sinkTableSQL);
                    System.out.println("jdbc 信息 : " + jdbcInfo.toString());

                    // 1.5 应用查询，创建Table对象 ，同时获得Table Schema信息
                    Table table = bsTableEnv.sqlQuery(query);
                    TableSchema schema = table.getSchema();
                    System.out.println();
                    System.out.print("Table " + tableName + " Schema : ");
                    table.printSchema();
                    System.out.println("Register Query : " + query);
                    System.out.println();

                    // 1.6 遍历schema对象，将列明信息 存入 columns 列表
                    List<String> columns = new ArrayList<>();
                    for(TableColumn column :schema.getTableColumns()){
                        columns.add(column.getName());
                    }



                    // 2 拼接流式算子任务链

                    bsTableEnv.toRetractStream(table, Row.class) // 2.1 将Table对象装换为DataStream对象
                            .filter(new FilterFunction<Tuple2<Boolean, Row>>() { // 2.2 过滤 true 更新对象信息
                                @Override
                                public boolean filter(Tuple2<Boolean, Row> tuple2) throws Exception {
                                    return tuple2.f0;
                                }
                            }).map(new RichMapFunction<Tuple2<Boolean,Row>, JSONObject>() { // 2.3 将Row对象转换为JSONObject对象

                                List<String> schema = null;

                                @Override
                                public void open(Configuration parameters) throws Exception {
                                    super.open(parameters);
                                    schema = columns;
                                }

                                @Override
                                public JSONObject map(Tuple2<Boolean, Row> event) throws Exception {

                                    JSONObject json = new JSONObject();
                                    int i = 0;
                                    for(String column :schema){
                                        String value = null;
                                        Object obj = event.f1.getField(i);
                                        if(ObjectUtils.isEmpty(obj)){
                                            value = "";
                                        }else {
                                            value = obj.toString();
                                        }
                                        json.put(column,value);
                                        i++;
                                    }

                                    return json;
                                }

                    }).addSink(new RichSinkFunction<JSONObject>() { // 2.4 将JSONObject对象持久化到MySQL数据库

                        // 2.4.1 初始化 元数据 信息
                        List<String> schema = null; // sql 结果schema
                        List<String> pkeys = null; // sql用于更新的主键集
                        Map<String,String> jdbc = null; // jdbc连接信息
                        Connection connection = null; // mysql连接
                        String sinkTable = null; //

                        // 该SQL对应的 jdbc语句
                        String select = null;
                        String insert = null;
                        String delete = null;
                        String update = null;

                        // 主键集合长度 和 schema长度
                        int keyLength = 0;
                        int schemaLength = 0;

                        // 封装SQL参数
                        String[] selectParams = null;
                        String[] insertParams = null;
                        String[] updateParams = null;
                        String[] deleteParams = null;



                        @Override
                        public void open(Configuration parameters) throws Exception {
                            super.open(parameters);

                            schema = columns;
                            pkeys = primaryKeys;
                            jdbc = jdbcInfo;

                            String driver = jdbc.get("driver");
                            String url = jdbc.get("url");
                            String user = jdbc.get("user");
                            String password = jdbc.get("password");

                            try{
                                Class.forName(driver);
                                connection = DriverManager.getConnection(url, user, password);
                            }catch (Exception e){
                                e.printStackTrace();
                            }

                            sinkTable = tableName;

                            keyLength = pkeys.size();
                            schemaLength = schema.size();

                            selectParams = new String[keyLength];
                            insertParams = new String[schemaLength];
                            updateParams = new String[schemaLength+keyLength];
                            deleteParams = new String[keyLength];

                            // 拼接SQL语句
                            // 1 拼接 select delete 和 where 语句
                            String whereCondition = "";
                            StringBuilder tmpSelect = new StringBuilder();
                            StringBuilder tmpDelete = new StringBuilder();
                            StringBuilder tmpWhereCondition = new StringBuilder();

                            tmpSelect.append("select 1 from ").append(sinkTable);
                            tmpDelete.append("delete from ").append(sinkTable);
                            tmpWhereCondition.append(" where ");
                            for(String key :pkeys){
                                tmpWhereCondition.append(key).append(" = ? and ");
                            }
                            whereCondition = tmpWhereCondition.toString().substring(0, tmpWhereCondition.length() - 5);

                            tmpSelect.append(whereCondition);
                            select = new String(tmpSelect);
                            System.out.println(" ********* 查询SQL : " + select);
                            System.out.println();

                            tmpDelete.append(whereCondition);
                            delete = new String(tmpDelete);
                            System.out.println(" ********* 删除SQL : " + delete);
                            System.out.println();



                            // 2 拼接 insert update 语句 ,安装schema list顺序
                            StringBuilder tmpInsert = new StringBuilder();
                            StringBuilder tmpUpdate = new StringBuilder();
                            StringBuilder tmpInsertColumnList = new StringBuilder();
                            StringBuilder tmpInsertValueList = new StringBuilder();
                            StringBuilder tmpUpdateSets = new StringBuilder();
                            tmpUpdateSets.append(" set ");

                            for(String column :schema){
                                tmpInsertColumnList.append(column).append(",");
                                tmpInsertValueList.append("?,");
                                tmpUpdateSets.append(column).append(" = ? ,");
                            }
                            tmpInsertColumnList.deleteCharAt(tmpInsertColumnList.length() - 1);
                            tmpInsertValueList.deleteCharAt(tmpInsertValueList.length() - 1);
                            tmpUpdateSets.deleteCharAt(tmpUpdateSets.length() - 1);
                            tmpInsert.append("insert into ").append(sinkTable).append("(").append(tmpInsertColumnList).append(")values(").append(tmpInsertValueList).append(")");
                            insert = new String(tmpInsert);
                            System.out.println(" ********* 插入SQL : " + insert);
                            System.out.println();

                            tmpUpdate.append("update ").append(sinkTable).append(tmpUpdateSets).append(whereCondition);
                            update = new String(tmpUpdate);
                            System.out.println(" *********更新SQL : " + update);
                            System.out.println();


                        }


                        @Override
                        public void invoke(JSONObject json, Context context) throws Exception {


                            int i = 0;
                            for(String key :pkeys){
                                String value = json.getString(key);
                                selectParams[i] = value;
                                deleteParams[i] = value;
                                updateParams[schemaLength + i] = value;
                                i++;
                            }

                            int j = 0;
                            for(String column :schema){
                                String value = json.getString(column);
                                insertParams[j] = value;
                                updateParams[j] = value;
                                j++;
                            }

                            // 执行SQL 语句
                            if(FlinkSQLJDBCTools.isExistsOfRecord(connection,select,selectParams)){
                                FlinkSQLJDBCTools.executeUpdate(connection,update,updateParams);
                            }else {
                                FlinkSQLJDBCTools.executeUpdate(connection,insert,insertParams);
                            }

                        }


                        @Override
                        public void close() throws Exception {
                            super.close();
                            connection.close();
                        }

                    })
                    .name("Write MySQL"); // 2.5 给算子命名


                } else{

                    bsTableEnv.sqlUpdate(sql.toString());
                    System.out.println(sql.toString() + "\n");

                }

            }

        }


        // 6 启动Flink任务
        try {
            bsTableEnv.execute("Stream for SQL");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    /**
     * 纯SQL流计算示例-简版
     *
     * @param sqlFile
     */
    public static void sqlStreamWithSQLFileExample(String sqlFile) {

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

        // 2 解析SQL文件 return {params,functions,tables,views,etlSQL}
        List[] sqlList = SQLUtils.parseSQLScript(sqlFile);


        // 3.1  声明时间语义
        if (SQLUtils.getTimeCharacteristic(sqlList[0]) == 2) {
            bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            System.out.println("程序以Event Time语义执行.");
            System.out.println();
        } else {
            bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
            System.out.println("程序以Processing Time语义执行.");
            System.out.println();
        }


        /**
         * 3.2 注册udf
         */
        for (Object sql : sqlList[1]) {

            String[] elements = SQLUtils.parseFunctionSQL(sql.toString());
            String functionName = elements[0];
            String functionClass = elements[1];

            try {
                Object object = Class.forName(functionClass).newInstance();
                if (object instanceof ScalarFunction) {
                    bsTableEnv.registerFunction(functionName, (ScalarFunction) object);
                } else if (object instanceof TableFunction) {
                    bsTableEnv.registerFunction(functionName, (TableFunction) object);
                } else if (object instanceof AggregateFunction) {
                    bsTableEnv.registerFunction(functionName, (AggregateFunction) object);
                } else if (object instanceof TableAggregateFunction) {
                    bsTableEnv.registerFunction(functionName, (TableAggregateFunction) object);
                } else {
                    logger.error("用户自定义语句不能创建函数 ： " + sql + "\n");
                    continue;
                }

                System.out.println("UDF : " + functionName + " 注册成功." + "\n");
            } catch (Exception e) {
                System.out.println("UDF : " + functionName + " 注册成功." + "\n");
                logger.error(e);
            }

        }


        /**
         * 3.3 注册table schema
         */
        for (Object sql : sqlList[2]) {

            bsTableEnv.sqlUpdate(sql.toString());
            System.out.println(sql.toString() + "\n");

        }


        /**
         * 3.5 注册etl逻辑
         */
        for (Object sql : sqlList[4]) {

            if (sql.toString().trim().toLowerCase().startsWith("insert")) {
                bsTableEnv.sqlUpdate(sql.toString());
                System.out.println(sql.toString() + "\n");
            }

        }


        // 4 启动Flink任务
        try {
            bsTableEnv.execute("Stream for SQL");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
