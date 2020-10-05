package com.pgman.goku.sql.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;


public final class JavaStructuredNetworkWordCountWindowed {

  public static void main(String[] args) throws Exception {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

    String host = args[0];
    int port = Integer.parseInt(args[1]);

    int windowSize = Integer.parseInt(args[2]);
    int slideSize = (args.length == 3) ? windowSize : Integer.parseInt(args[3]);

    if (slideSize > windowSize) {
      System.err.println("<slide duration> must be less than or equal to <window duration>");
    }

    String windowDuration = windowSize + " seconds";
    String slideDuration = slideSize + " seconds";

    SparkSession spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("JavaStructuredNetworkWordCountWindowed")
      .getOrCreate();

    // Create DataFrame representing the stream of input lines from connection to host:port
    Dataset<Row> lines = spark
      .readStream()
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load();

    // Split the lines into words, retaining timestamps
    Dataset<Row> words = lines
      .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
      .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
          List<Tuple2<String, Timestamp>> result = new ArrayList<>();
          for (String word : t._1.split(" ")) {
            result.add(new Tuple2<>(word, t._2));
          }
          return result.iterator();
        },
        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
      ).toDF("word", "timestamp");

    // Group the data by window and word and compute the count of each group
    Dataset<Row> windowedCounts = words
            .withWatermark("timestamp", "10 minutes")
            .groupBy(
                  functions.window(words.col("timestamp"), windowDuration, slideDuration),
                  words.col("word")
             ).count().orderBy("window");

    // Start running the query that prints the windowed word counts to the console
    StreamingQuery query = windowedCounts.writeStream()
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start();

    query.awaitTermination();
  }

}
