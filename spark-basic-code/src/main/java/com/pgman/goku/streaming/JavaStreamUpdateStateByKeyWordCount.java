package com.pgman.goku.streaming;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

public class JavaStreamUpdateStateByKeyWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

        // 0、设置hadoop环境，没有必要注释掉这行
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("UpdateStateByKeyWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
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

        // 以下代码实现了离线WordCount逻辑

        // Split each line into words
        JavaDStream<String> words = streamValues.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // 使用updateStateByKey算子进行单词统计
        JavaPairDStream<String, Integer> wordCount = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {

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

        wordCount.print();

        jssc.start(); // Start the computation
        jssc.awaitTermination();
        jssc.close();

    }

}
