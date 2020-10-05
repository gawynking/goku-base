package com.pgman.goku.sql.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

/**
 *
 * Structured Streaming：
 * 结构化流是基于Spark SQL引擎构建的可伸缩且容错的流处理引擎。您可以像对静态数据进行批处理计算一样来表示流计算。
 * 当流数据继续到达时，Spark SQL引擎将负责递增地，连续地运行它并更新最终结果。
 * 您可以在Scala，Java，Python或R中使用Dataset / DataFrame API来表示流聚合，事件时间窗口，流到批处理联接等。
 * 计算在同一优化的Spark SQL引擎上执行。最后，系统通过检查点和预写日志来确保端到端的一次容错保证。
 * 简而言之，结构化流提供了快速，可扩展，容错，端到端的精确一次流处理，而用户无需推理流。
 *
 * 结构化流传输中的关键思想是将实时数据流视为被连续添加的表。这导致了一个新的流处理模型，该模型与批处理模型非常相似。您将像在静态表上一样将流计算表示为类似于批处理的标准查询，Spark 在无界输入表上将其作为增量查询运行。
 *
 * spark处理延迟到达数据：spark天然处理延迟数据，当延迟数据到达时，更新延时数据
 *
 * 引擎使用检查点和预写日志来记录每个触发器中正在处理的数据的偏移范围。
 */

public final class JavaStructuredNetworkWordCount {

  public static void main(String[] args) throws Exception {

    /**
     * 1、idea环境运行引入hadoop环境，否则会报错hadoop文件不存在
     */
    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

    String host = "192.168.72.131";
    int port = 9999;

    /**
     * 2、构建SparkSession对象
     */
    SparkSession spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("JavaStructuredNetworkWordCount")
      .getOrCreate();

    /**
     * 3、构建流式矿建
     *
     * 通过 SparkSession.readStream() 返回DataStreamReader对象创建 Datasets and DataFrames
     * 1） Create DataFrame representing the stream of input lines from connection to localhost:9999
     */
    Dataset<Row> lines = spark
      .readStream()
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load();

    lines.isStreaming();
    lines.printSchema();

    // Split the lines into words
    Dataset<String> words = lines.as(Encoders.STRING()).flatMap(
        (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
        Encoders.STRING());

    // Generate running word count
    Dataset<Row> wordCounts = words.groupBy("value").count();

    /**
     * 4、Start running the query that prints the running counts to the console
     *
     *
     * 知识点：outputMode
     * Complete Mode - 整个更新的结果表将被写入外部存储器。由存储连接器决定如何处理整个表的写入。
     * Append Mode - 仅将自上次触发以来追加在结果表中的新行写入外部存储器。这仅适用于预期结果表中现有行不会更改的查询。
     * Update Mode - 仅自上次触发以来在结果表中已更新的行将被写入外部存储（自Spark 2.1.1起可用）。请注意，这与完成模式的不同之处在于此模式仅输出自上次触发以来已更改的行。如果查询不包含聚合，它将等同于追加模式。
     *
     */
    StreamingQuery query = wordCounts.writeStream()
      .outputMode("complete")
      .format("console")
      .start();

    query.awaitTermination();
  }
}
