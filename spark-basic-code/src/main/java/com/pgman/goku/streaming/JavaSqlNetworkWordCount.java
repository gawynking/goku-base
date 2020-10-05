/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pgman.goku.streaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 * Use DataFrames and SQL to count words in UTF8 encoded, '\n' delimited text received from the
 * network every second.
 *
 * Usage: JavaSqlNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.JavaSqlNetworkWordCount localhost 9999`
 */
public final class JavaSqlNetworkWordCount {

  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    // 0、设置hadoop环境，没有必要注释掉这行
    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf()
            .setAppName("JavaSqlNetworkWordCount")
            .setMaster("local[*]");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(20));
    jssc.checkpoint(".");

    /**
     * 创建kafka direct数据源
     */
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", "test.server:9092");
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "spark-test");
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("enable.auto.commit", false);

    Collection<String> topics = Arrays.asList("spark-test");

    JavaInputDStream<ConsumerRecord<String, String>> stream =
            KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );

    JavaDStream<String> streamValues = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value())).map(record -> record._2);

    JavaDStream<String> words = streamValues.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());


    // Convert RDDs of the words DStream to DataFrame and run SQL query
    words.foreachRDD((rdd, time) -> {
      SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());

      // Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
      JavaRDD<JavaRecord> rowRDD = rdd.map(word -> {
        JavaRecord record = new JavaRecord();
        record.setWord(word);
        return record;
      });

      Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, JavaRecord.class);

      // Creates a temporary view using the DataFrame
      wordsDataFrame.createOrReplaceTempView("words");

      // Do word count on table using SQL and print it
      Dataset<Row> wordCountsDataFrame =
          spark.sql("select word, count(distinct word) as total from words group by word order by total desc");

      System.out.println("========= " + time + "=========");
      wordCountsDataFrame.show();
    });

    jssc.start();
    jssc.awaitTermination();

  }
}

/** Lazily instantiated singleton instance of SparkSession */
class JavaSparkSessionSingleton {
  private static transient SparkSession instance = null;
  public static SparkSession getInstance(SparkConf sparkConf) {
    if (instance == null) {
      instance = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate();
    }
    return instance;
  }
}
