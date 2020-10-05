package com.pgman.goku.streaming;

import java.sql.PreparedStatement;
import java.util.*;

import com.pgman.goku.util.ConnectionPool;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import com.mysql.jdbc.Connection;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

public class JavaSteamPersistWordCount {

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

		// Split each line into words
		JavaDStream<String> words = streamValues.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) throws Exception {
				return Arrays.asList(s.split(" ")).iterator();
			}
		});

		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		// 使用updatestatebykey算子进行单词统计
		JavaPairDStream<String, Integer> wordCount = pairs
				.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
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

		/**
		 *  写数据到mysql:
		 *  知识点 ： foreachRDD 设计模式
		 *
		 *  1)错误设计方式1：
		 *     dstream.foreachRDD(rdd -> {
		 *      Connection connection = createNewConnection(); // 在驱动程序中创建连接
		 *      rdd.foreach(record -> {
		 *        connection.send(record); // executed at the worker
		 *      });
		 *    });
		 *
		 *  2)错误设计方式2：
		 *     dstream.foreachRDD(rdd -> {
		 *      rdd.foreach(record -> {
		 *        Connection connection = createNewConnection(); // 每个记录创建一个新的连接
		 *        connection.send(record);
		 *        connection.close();
		 *      });
		 *    });
		 *
		 *  3)正确设计模式1(手动创建连接)
		 *     dstream.foreachRDD(rdd -> {
		 *      rdd.foreachPartition(partitionOfRecords -> {
		 *        Connection connection = createNewConnection(); // 程序中创建连接
		 *        while (partitionOfRecords.hasNext()) {
		 *          connection.send(partitionOfRecords.next());
		 *        }
		 *        connection.close(); // 程序中关闭连接
		 *      });
		 *    });
		 *
		 *  4)优化后设计模式(使用连接池)
		 *    dstream.foreachRDD(rdd -> {
		 *     rdd.foreachPartition(partitionOfRecords -> {
		 *       // ConnectionPool is a static, lazily initialized pool of connections
		 *       Connection connection = ConnectionPool.getConnection();
		 *       while (partitionOfRecords.hasNext()) {
		 *         connection.send(partitionOfRecords.next());
		 *       }
		 *       ConnectionPool.returnConnection(connection); // return to the pool for future reuse
		 *     });
		 *   });
		 */
		wordCount.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {

			public void call(JavaPairRDD<String, Integer> wc) throws Exception {
				// TODO Auto-generated method stub
				wc.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {

					public void call(Iterator<Tuple2<String, Integer>> t) throws Exception {
						// TODO Auto-generated method stub
						Connection conn = (Connection) ConnectionPool.getConnection();

						while (t.hasNext()) {
							Tuple2 wc = t.next();
							String sql = "insert into mybatis.word_count(word,cnt) values( '" + wc._1 + "','"
									+ wc._2 + "')";
							PreparedStatement pstmt = conn.prepareStatement(sql);
							pstmt.executeUpdate();
							pstmt.close();
						}

						ConnectionPool.returnConnection(conn);
					}

				});
			}

		});


		jssc.start(); // Start the computation
		jssc.awaitTermination();
		jssc.close();

	}

}
