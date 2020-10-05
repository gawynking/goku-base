package com.pgman.goku.streaming;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

public class HotWordsTopNStat {

	public static void main(String[] args) throws Exception {

		hotWordsTopNStat();

	}


	public static void hotWordsTopNStat() throws Exception {

		// TODO Auto-generated method stub

		// Create a local StreamingContext with two working thread and batch
		// interval of 1 second
		SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("TransformBlacklist");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

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

		JavaDStream<String> searchWordsDSteam = streamValues.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1.split(" ")[2] + "_" + v1.split(" ")[1];
            }
        });

		JavaPairDStream<String, Integer> searchWordsPairsDStream = searchWordsDSteam
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, Integer>(t, 1);
					}
				});

		JavaPairDStream<String, Integer> searchWordsReduceByKeyDStream = searchWordsPairsDStream
				.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				}, Durations.seconds(60), Durations.seconds(10));



		searchWordsReduceByKeyDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {

			public void call(JavaPairRDD<String, Integer> searchWords) throws Exception {

				JavaRDD<Row> searchWordsROW = searchWords.map(new Function<Tuple2<String, Integer>, Row>() {
					public Row call(Tuple2<String, Integer> v1) throws Exception {

						// TODO Auto-generated method stub
						String category = v1._1.split("_")[1];
						String product = v1._1.split("_")[0];
						Integer count = v1._2;

						return RowFactory.create(category, product, count);
					}
				});

				String warehouseLocation = "hdfs://chavin.king:8020/user/hive/warehouse";
				SparkSession spark = SparkSession
                        .builder()
                        .master("spark://chavin.king:7077")
						.appName("Java Spark Hive Example")
                        .config("spark.sql.warehouse.dir", warehouseLocation)
						// .enableHiveSupport() // 支持hive
						.getOrCreate();

				List<StructField> structFields = Arrays.asList(
						DataTypes.createStructField("category", DataTypes.StringType, true),
						DataTypes.createStructField("product", DataTypes.StringType, true),
						DataTypes.createStructField("click_count", DataTypes.IntegerType, true));
				StructType structType = DataTypes.createStructType(structFields);

				Dataset<Row> searchWordsDF = spark.createDataFrame(searchWordsROW, structType);

				searchWordsDF.createOrReplaceTempView("product_click");

				String sql = "SELECT " +
                                    "category," +
                                    "product," +
                                    "click_count " +
                                "FROM (" +
                                    "SELECT " +
                                    "category," +
                                    "product," +
                                    "click_count," +
                                    "row_number() OVER (PARTITION BY category ORDER BY click_count DESC) rank "	+
                                    "FROM product_click" +
                                ") tmp " +
                                "WHERE rank<=3";

				Dataset<Row> resultDF = spark.sql(sql);

				resultDF.show();
				Thread.sleep(10000);

//				// Saving data to a JDBC source
//				resultDF.write().format("jdbc").option("url", "jdbc:mysql://192.168.72.1:3306/mybatis")
//						.option("dbtable", "sparkdb.product_click").option("user", "root").option("password", "mysql")
//						.save();

				spark.close();

			}
		});

		jssc.start(); // Start the computation
		jssc.awaitTermination();

	}

}
