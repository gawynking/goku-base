package com.pgman.goku.core;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 取最大的前3个数字
 * @author Administrator
 *
 */
public class Top3 {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf()
				.setAppName("Top3")
				.setMaster("local");  
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//top.txt");
		
		JavaPairRDD<Integer, String> pairs = lines.mapToPair(
				
				new PairFunction<String, Integer, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Integer, String> call(String t) throws Exception {
						return new Tuple2<Integer, String>(Integer.valueOf(t), t);
					}
					
				});
		
		JavaPairRDD<Integer, String> sortedPairs = pairs.sortByKey(false);
		
		JavaRDD<Integer> sortedNumbers = sortedPairs.map(
				
				new Function<Tuple2<Integer,String>, Integer>() {

					private static final long serialVersionUID = 1L;

					public Integer call(Tuple2<Integer, String> v1) throws Exception {
						return v1._1;
					}
					
				});
		
		List<Integer> sortedNumberList = sortedNumbers.take(3);
		
		for(Integer num : sortedNumberList) {
			System.out.println(num);
		}
		
		sc.close();
	}
	
}
