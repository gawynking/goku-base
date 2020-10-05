package com.pgman.goku.sql.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

public final class JavaStructuredKafkaWordCount {

  public static void main(String[] args) throws Exception {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");
    String bootstrapServers = "test.server:9092";
    String topics = "spark-test";

    wordCountSQL(bootstrapServers,topics);

  }


  public static void wordCountNoSQL(String bootstrapServers,String topics) throws Exception{

    SparkSession spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("JavaStructuredKafkaWordCount")
            .getOrCreate();

    // Create DataSet representing the stream of input lines from kafka
    Dataset<String> lines = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrapServers)
            .option("subscribe", topics)
            .load()
            .selectExpr("CAST(value AS STRING)")
            .as(Encoders.STRING());


    // Generate running word count
    Dataset<Row> wordCounts = lines.flatMap(
            (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
            Encoders.STRING()).groupBy("value").count();

    // Start running the query that prints the running counts to the console
    StreamingQuery query = wordCounts.writeStream()
            .outputMode("complete")
            .format("console")
            .start();

    query.awaitTermination();
  }


  public static void wordCountSQL(String bootstrapServers,String topics) throws Exception{

    SparkSession spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("JavaStructuredKafkaWordCount")
            .getOrCreate();

    // Create DataSet representing the stream of input lines from kafka
    Dataset<String> lines = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrapServers)
            .option("subscribe", topics)
            .load()
            .selectExpr("CAST(value AS STRING)")
            .as(Encoders.STRING())
            .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                    Encoders.STRING());

    lines.isStreaming();
    lines.printSchema();

    lines.registerTempTable("words");

    // Generate running word count
    Dataset<Row> wordCounts = spark.sql("select value as word,count(1) from words group by value");

    // Start running the query that prints the running counts to the console
    StreamingQuery query = wordCounts.writeStream()
            .outputMode("complete")
            .format("console")
            .start();

    query.awaitTermination();

  }

}
