package com.pgman.goku.core;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;

/**
 * 批数据处理demo
 * local版word count程序，计算结果降序排序
 */
public class WordCountLocal {

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        // 0、设置hadoop环境，没有必要注释掉这行
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        // 1、初始化spark应用
        SparkConf conf = new SparkConf().setAppName("WordCountLocal").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 2、读取文件
        JavaRDD<String> lines = sc.textFile("E:\\goku\\spark-basic-code\\src\\main\\files\\word.txt");
        // 缓存rdd数据
        lines.persist(StorageLevel.MEMORY_ONLY());

        // 3、将每一行切割成单词
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }

        });
        // 缓存数据
        words.cache();

        // 4、将每个单词映射成(word,1)格式
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }

        });

        // 5、计算每个单词出现次数
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }

        });

        // 6、反转key value
        JavaPairRDD<Integer, String> reverseWordCounts = wordCounts
                .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                        return new Tuple2<Integer, String>(t._2, t._1);
                    }
                });

        // 7、按照key值大小排序
        JavaPairRDD<Integer, String> sortWordCounts = reverseWordCounts.sortByKey(false);

        // 8、反转key value
        JavaPairRDD<String, Integer> reverseSortWordCounts = sortWordCounts
                .mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                        return new Tuple2<String, Integer>(t._2, t._1);
                    }
                });

        // 9、foreach打印输出
        reverseSortWordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
            }

        });

        // 10、关闭SparkContext
        sc.close();

    }

}
