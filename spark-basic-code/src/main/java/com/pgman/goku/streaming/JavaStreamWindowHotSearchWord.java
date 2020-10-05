package com.pgman.goku.streaming;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

public class JavaStreamWindowHotSearchWord {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

        // 0、设置hadoop环境，没有必要注释掉这行
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        // Create a local StreamingContext with two working thread and batch
        // interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("TransformBlacklist");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
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


        JavaDStream<String> words = streamValues.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> searchWordPairDStream = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(t,1);
			}
		});

        /**
         * reduceByKeyAndWindow 执行窗口计算
         */
		JavaPairDStream<String, Integer> searchWordReduceByKeyDStream = searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		}, Durations.seconds(10), Durations.seconds(5));



		JavaPairDStream<String, Integer> searchHotWordsTop3DStream = searchWordReduceByKeyDStream.transformToPair(
				new Function<JavaPairRDD<String,Integer>, JavaPairRDD<String,Integer>>() {
					public JavaPairRDD<String,Integer> call(JavaPairRDD<String, Integer> searchHotWordRDD) throws Exception {
						// TODO Auto-generated method stub

						JavaPairRDD<Integer, String> searchHotWordToPairsRDD = searchHotWordRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
							public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
								// TODO Auto-generated method stub
								return new Tuple2<Integer, String>(t._2,t._1);
							}
						});

						JavaPairRDD<Integer, String> searchHotWordDESCRDD = searchHotWordToPairsRDD.sortByKey(false);

						JavaPairRDD<String, Integer> searchHotWordASCRDD = searchHotWordDESCRDD.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
							public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
								// TODO Auto-generated method stub
								return new Tuple2<String, Integer>(t._2,t._1);
							}
						});

						List<Tuple2<String, Integer>> searchTop3Word = searchHotWordASCRDD.take(3);

						for(@SuppressWarnings("rawtypes") Tuple2 topWord : searchTop3Word){
							System.out.println(topWord._1 + ":" + topWord._2);
						}

						return searchHotWordASCRDD;
					}
				});


        searchHotWordsTop3DStream.print();

		jssc.start(); // Start the computation
		jssc.awaitTermination();
		jssc.close();
	}

}
