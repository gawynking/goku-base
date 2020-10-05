package com.pgman.goku.sql.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

/**
 * 程序运行报错：
 *
 * 20/02/04 10:28:35 ERROR MicroBatchExecution: Query [id = f184b4b5-bdd4-40c5-a07b-e2fb0f69cd57, runId = 4a8a6e9e-6427-43eb-90e6-f21c7e9581bd] terminated with error
 * java.lang.IllegalArgumentException: Illegal pattern component: XXX
 * 	at org.apache.commons.lang3.time.FastDatePrinter.parsePattern(FastDatePrinter.java:282)
 * 	at org.apache.commons.lang3.time.FastDatePrinter.init(FastDatePrinter.java:149)
 * 	at org.apache.commons.lang3.time.FastDatePrinter.<init>(FastDatePrinter.java:142)
 * 	at org.apache.commons.lang3.time.FastDateFormat.<init>(FastDateFormat.java:384)
 * 	at org.apache.commons.lang3.time.FastDateFormat.<init>(FastDateFormat.java:369)
 * 	at org.apache.commons.lang3.time.FastDateFormat$1.createInstance(FastDateFormat.java:91)
 * 	at org.apache.commons.lang3.time.FastDateFormat$1.createInstance(FastDateFormat.java:88)
 * 	at org.apache.commons.lang3.time.FormatCache.getInstance(FormatCache.java:82)
 * 	at org.apache.commons.lang3.time.FastDateFormat.getInstance(FastDateFormat.java:165)
 * 	at org.apache.spark.sql.execution.datasources.csv.CSVOptions.<init>(CSVOptions.scala:140)
 * 	at org.apache.spark.sql.execution.datasources.csv.CSVOptions.<init>(CSVOptions.scala:45)
 * 	at org.apache.spark.sql.execution.datasources.csv.CSVFileFormat.buildReader(CSVFileFormat.scala:109)
 * 	at org.apache.spark.sql.execution.datasources.FileFormat.buildReaderWithPartitionValues(FileFormat.scala:130)
 * 	at org.apache.spark.sql.execution.datasources.FileFormat.buildReaderWithPartitionValues$(FileFormat.scala:121)
 * 	at org.apache.spark.sql.execution.datasources.TextBasedFileFormat.buildReaderWithPartitionValues(FileFormat.scala:165)
 * 	at org.apache.spark.sql.execution.FileSourceScanExec.inputRDD$lzycompute(DataSourceScanExec.scala:316)
 * 	at org.apache.spark.sql.execution.FileSourceScanExec.inputRDD(DataSourceScanExec.scala:305)
 * 	at org.apache.spark.sql.execution.FileSourceScanExec.inputRDDs(DataSourceScanExec.scala:327)
 * 	at org.apache.spark.sql.execution.WholeStageCodegenExec.doExecute(WholeStageCodegenExec.scala:627)
 * 	at org.apache.spark.sql.execution.SparkPlan.$anonfun$execute$1(SparkPlan.scala:131)
 * 	at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeQuery$1(SparkPlan.scala:155)
 * 	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
 * 	at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:152)
 * 	at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:127)
 * 	at org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec.doExecute(WriteToDataSourceV2Exec.scala:57)
 * 	at org.apache.spark.sql.execution.SparkPlan.$anonfun$execute$1(SparkPlan.scala:131)
 * 	at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeQuery$1(SparkPlan.scala:155)
 * 	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
 * 	at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:152)
 * 	at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:127)
 * 	at org.apache.spark.sql.execution.SparkPlan.getByteArrayRdd(SparkPlan.scala:247)
 * 	at org.apache.spark.sql.execution.SparkPlan.executeCollect(SparkPlan.scala:296)
 * 	at org.apache.spark.sql.Dataset.collectFromPlan(Dataset.scala:3389)
 * 	at org.apache.spark.sql.Dataset.$anonfun$collect$1(Dataset.scala:2788)
 * 	at org.apache.spark.sql.Dataset.$anonfun$withAction$2(Dataset.scala:3370)
 * 	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:78)
 * 	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:125)
 * 	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:73)
 * 	at org.apache.spark.sql.Dataset.withAction(Dataset.scala:3370)
 * 	at org.apache.spark.sql.Dataset.collect(Dataset.scala:2788)
 * 	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runBatch$15(MicroBatchExecution.scala:540)
 * 	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:78)
 * 	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:125)
 * 	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:73)
 * 	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runBatch$14(MicroBatchExecution.scala:536)
 * 	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:351)
 * 	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:349)
 * 	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:58)
 * 	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runBatch(MicroBatchExecution.scala:535)
 * 	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:198)
 * 	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
 * 	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:351)
 * 	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:349)
 * 	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:58)
 * 	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:166)
 * 	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:56)
 * 	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:160)
 * 	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:281)
 * 	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:193)
 * Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: Illegal pattern component: XXX
 * === Streaming Query ===
 * Identifier: [id = f184b4b5-bdd4-40c5-a07b-e2fb0f69cd57, runId = 4a8a6e9e-6427-43eb-90e6-f21c7e9581bd]
 * Current Committed Offsets: {}
 * Current Available Offsets: {FileStreamSource[file:/E:/text/test]: {"logOffset":0}}
 *
 * Current State: ACTIVE
 * Thread State: RUNNABLE
 *
 * Logical Plan:
 * Project [name#0, age#1]
 * +- SubqueryAlias `people`
 *    +- StreamingExecutionRelation FileStreamSource[file:/E:/text/test], [name#0, age#1]
 *
 * 	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:302)
 * 	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:193)
 * Caused by: java.lang.IllegalArgumentException: Illegal pattern component: XXX
 * 	at org.apache.commons.lang3.time.FastDatePrinter.parsePattern(FastDatePrinter.java:282)
 * 	at org.apache.commons.lang3.time.FastDatePrinter.init(FastDatePrinter.java:149)
 * 	at org.apache.commons.lang3.time.FastDatePrinter.<init>(FastDatePrinter.java:142)
 * 	at org.apache.commons.lang3.time.FastDateFormat.<init>(FastDateFormat.java:384)
 * 	at org.apache.commons.lang3.time.FastDateFormat.<init>(FastDateFormat.java:369)
 * 	at org.apache.commons.lang3.time.FastDateFormat$1.createInstance(FastDateFormat.java:91)
 * 	at org.apache.commons.lang3.time.FastDateFormat$1.createInstance(FastDateFormat.java:88)
 * 	at org.apache.commons.lang3.time.FormatCache.getInstance(FormatCache.java:82)
 * 	at org.apache.commons.lang3.time.FastDateFormat.getInstance(FastDateFormat.java:165)
 * 	at org.apache.spark.sql.execution.datasources.csv.CSVOptions.<init>(CSVOptions.scala:140)
 * 	at org.apache.spark.sql.execution.datasources.csv.CSVOptions.<init>(CSVOptions.scala:45)
 * 	at org.apache.spark.sql.execution.datasources.csv.CSVFileFormat.buildReader(CSVFileFormat.scala:109)
 * 	at org.apache.spark.sql.execution.datasources.FileFormat.buildReaderWithPartitionValues(FileFormat.scala:130)
 * 	at org.apache.spark.sql.execution.datasources.FileFormat.buildReaderWithPartitionValues$(FileFormat.scala:121)
 * 	at org.apache.spark.sql.execution.datasources.TextBasedFileFormat.buildReaderWithPartitionValues(FileFormat.scala:165)
 * 	at org.apache.spark.sql.execution.FileSourceScanExec.inputRDD$lzycompute(DataSourceScanExec.scala:316)
 * 	at org.apache.spark.sql.execution.FileSourceScanExec.inputRDD(DataSourceScanExec.scala:305)
 * 	at org.apache.spark.sql.execution.FileSourceScanExec.inputRDDs(DataSourceScanExec.scala:327)
 * 	at org.apache.spark.sql.execution.WholeStageCodegenExec.doExecute(WholeStageCodegenExec.scala:627)
 * 	at org.apache.spark.sql.execution.SparkPlan.$anonfun$execute$1(SparkPlan.scala:131)
 * 	at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeQuery$1(SparkPlan.scala:155)
 * 	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
 * 	at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:152)
 * 	at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:127)
 * 	at org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec.doExecute(WriteToDataSourceV2Exec.scala:57)
 * 	at org.apache.spark.sql.execution.SparkPlan.$anonfun$execute$1(SparkPlan.scala:131)
 * 	at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeQuery$1(SparkPlan.scala:155)
 * 	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
 * 	at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:152)
 * 	at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:127)
 * 	at org.apache.spark.sql.execution.SparkPlan.getByteArrayRdd(SparkPlan.scala:247)
 * 	at org.apache.spark.sql.execution.SparkPlan.executeCollect(SparkPlan.scala:296)
 * 	at org.apache.spark.sql.Dataset.collectFromPlan(Dataset.scala:3389)
 * 	at org.apache.spark.sql.Dataset.$anonfun$collect$1(Dataset.scala:2788)
 * 	at org.apache.spark.sql.Dataset.$anonfun$withAction$2(Dataset.scala:3370)
 * 	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:78)
 * 	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:125)
 * 	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:73)
 * 	at org.apache.spark.sql.Dataset.withAction(Dataset.scala:3370)
 * 	at org.apache.spark.sql.Dataset.collect(Dataset.scala:2788)
 * 	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runBatch$15(MicroBatchExecution.scala:540)
 * 	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:78)
 * 	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:125)
 * 	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:73)
 * 	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runBatch$14(MicroBatchExecution.scala:536)
 * 	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:351)
 * 	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:349)
 * 	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:58)
 * 	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runBatch(MicroBatchExecution.scala:535)
 * 	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:198)
 * 	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
 * 	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:351)
 * 	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:349)
 * 	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:58)
 * 	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:166)
 * 	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:56)
 * 	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:160)
 * 	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:281)
 * 	... 1 more
 * 20/02/04 10:28:35 INFO SparkContext: Invoking stop() from shutdown hook
 * 20/02/04 10:28:35 INFO SparkUI: Stopped Spark web UI at http://DESKTOP-316E6CT:4042
 */
public class javaStructuredCSVPeopleExample {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaStructuredKafkaWordCount")
                .getOrCreate();


        // Read all the csv files written atomically in a directory
        StructType userSchema = new StructType().add("name", "string").add("age", "integer");
        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ";")
                .schema(userSchema)      // Specify schema of the csv files
                .csv("E:\\text\\test");    // Equivalent to format("csv").load("/path/to/directory")

        csvDF.registerTempTable("people");

        Dataset<Row> result = spark.sql("select * from people");

        // Start running the query that prints the running counts to the console
        StreamingQuery query = result.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();
    }

}
