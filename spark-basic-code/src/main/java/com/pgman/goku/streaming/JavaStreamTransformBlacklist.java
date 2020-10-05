package com.pgman.goku.streaming;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 * 测试数据格式：
 * chavin dba
 * pgman dba
 * tom dba
 * ...
 */
public class JavaStreamTransformBlacklist {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

        // 0、设置hadoop环境，没有必要注释掉这行
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

		// Create a local StreamingContext with two working thread and batch
		// interval of 1 second
		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("TransformBlacklist");
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


		// 先做一份模拟的黑名单RDD
		List<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
		blacklist.add(new Tuple2<String, Boolean>("tom", true));

		JavaRDD<Tuple2<String, Boolean>> blacklistRDD = jssc.sparkContext().parallelize(blacklist);

		final JavaPairRDD<String, Boolean> blacklistPairsRDD = blacklistRDD
				.mapToPair(new PairFunction<Tuple2<String, Boolean>, String, Boolean>() {
					public Tuple2<String, Boolean> call(Tuple2<String, Boolean> t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, Boolean>(t._1, t._2);
					}
				});

		// 业务逻辑
		JavaPairDStream<String, String> userAdsClickLogParisDStream = streamValues
				.mapToPair(new PairFunction<String, String, String>() {
					public Tuple2<String, String> call(String t) throws Exception {
						return new Tuple2<String, String>(t.split(" ")[0], t);
					}
				});

        /**
         * transform算子可以将DStream转化为普通RDD，进而实现RDD算子方法
         */
		JavaDStream<String> userAdsClickLogDStreamTrue = userAdsClickLogParisDStream
				.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
					public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {
						// TODO Auto-generated method stub

						/**
						 * transform算子实现 流与静态数据集join操作
						 */
						JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> leftJoinRDD = userAdsClickLogRDD
								.leftOuterJoin(blacklistPairsRDD);

						JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRDD = leftJoinRDD
								.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {

							public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
								// TODO Auto-generated method stub

								if (v1._2._2.isPresent() && v1._2._2.get()) {
									return false;
								}
								return true;
							}
						});

						JavaRDD<String> resultRDD = filterRDD
								.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
							public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
								// TODO Auto-generated method stub
								return v1._2._1;
							}
						});

						return resultRDD;
					}
				});

		userAdsClickLogDStreamTrue.print();

        // Start the computation
		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}

}
