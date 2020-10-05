/*
 * 累加器，广播变量和检查点
 *
 * 实现检查点恢复功能
 */
package com.pgman.goku.streaming;

import java.io.File;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import com.google.common.io.Files;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.util.LongAccumulator;


/**
 * Use this singleton to get or register a Broadcast variable.
 * 使用单例模式get或register一个广播变量
 */
class JavaWordBlacklist {

  private static volatile Broadcast<List<String>> instance = null;

  public static Broadcast<List<String>> getInstance(JavaSparkContext jsc) {
    if (instance == null) {
      synchronized (JavaWordBlacklist.class) {
        if (instance == null) {
          List<String> wordBlacklist = Arrays.asList("a", "b", "c");
          instance = jsc.broadcast(wordBlacklist);
        }
      }
    }
    return instance;
  }
}

/**
 * Use this singleton to get or register an Accumulator.
 * 使用单例模式get或register一个累积器
 */
class JavaDroppedWordsCounter {

  private static volatile LongAccumulator instance = null;

  public static LongAccumulator getInstance(JavaSparkContext jsc) {
    if (instance == null) {
      synchronized (JavaDroppedWordsCounter.class) {
        if (instance == null) {
          instance = jsc.sc().longAccumulator("WordsInBlacklistCounter");
        }
      }
    }
    return instance;
  }
}


public final class JavaRecoverableNetworkWordCount {

  private static final Pattern SPACE = Pattern.compile(" ");

  private static JavaStreamingContext createContext(String serverIp, // kafka地址
                                                    String topic, // topic名称
                                                    String checkpointDirectory, // 检查点目录
                                                    String outputPath) { // 持久化目录

    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    System.out.println("Creating new context");
    File outputFile = new File(outputPath);
    if (outputFile.exists()) {
      outputFile.delete();
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaRecoverableNetworkWordCount").setMaster("local[*]");
    // Create the context with a 1 second batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
    jssc.checkpoint(checkpointDirectory);

    /**
     * 替换成kafka源
     * 创建kafka direct数据源
     */
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", serverIp);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "spark-test");
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("enable.auto.commit", false);

    Collection<String> topics = Arrays.asList(topic);

    JavaInputDStream<ConsumerRecord<String, String>> stream =
            KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );

    JavaDStream<String> lines = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value())).map(record -> record._2);

    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

    JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<String, Integer>(s, 1);
      }
    });

    JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(new Function2<List<Integer>, org.apache.spark.api.java.Optional<Integer>, org.apache.spark.api.java.Optional<Integer>>() {
      public org.apache.spark.api.java.Optional<Integer> call(List<Integer> values, org.apache.spark.api.java.Optional<Integer> state) throws Exception {

        int newValue = 0;

        if (state.isPresent()) {
          newValue = state.get();
        }

        for (int value : values) {
          newValue = newValue + value;
        }

        return Optional.of(newValue);

      }
    });

    // 设置检查点时间间隔
    wordCounts.checkpoint(new Duration(1000));

    /**
     * 遍历wordCounts
     */
    wordCounts.foreachRDD((rdd, time) -> {

      // Get or register 黑名单广播变量
      Broadcast<List<String>> blacklist =
          JavaWordBlacklist.getInstance(new JavaSparkContext(rdd.context()));
      // Get or register 删除单词计数器
      LongAccumulator droppedWordsCounter =
          JavaDroppedWordsCounter.getInstance(new JavaSparkContext(rdd.context()));

      // Use blacklist to drop words and use droppedWordsCounter to count them
      String counts = rdd.filter(wordCount -> {
        if (blacklist.value().contains(wordCount._1())) {
          droppedWordsCounter.add(wordCount._2());
          return false;
        } else {
          return true;
        }
      }).collect().toString();

      String output = "Counts at time " + time + " " + counts;
      System.out.println(output);

      System.out.println("Dropped " + droppedWordsCounter.value() + " word(s) totally");

      System.out.println("Appending to " + outputFile.getAbsolutePath());
      Files.append(output + "\n", outputFile, Charset.defaultCharset());

    });

    return jssc;

  }


  public static void main(String[] args) throws Exception {

    // 0、设置hadoop环境，没有必要注释掉这行
    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

    String serverIp = "test.server:9092";
    String topic = "spark-test";
    String checkpointDirectory = "E:\\text\\ck-test";
    String outputPath = "E:\\text\\out\\result";

    /**
     * 实现检查点恢复
     */
    JavaStreamingContext jssc =
      JavaStreamingContext.getOrCreate(checkpointDirectory, new Function0<JavaStreamingContext>() {
        @Override
        public JavaStreamingContext call() throws Exception {
          return createContext(serverIp, topic, checkpointDirectory, outputPath);
        }
      });

    jssc.start();
    jssc.awaitTermination();
  }

}
