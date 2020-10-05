package com.pgman.goku.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * RDD持久化
 * 数量下，没有测试出效果
 * @author Administrator
 */
public class SparkPersist {

    public static void main(String[] args) {

        // 0、设置hadoop环境，没有必要注释掉这行
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        // 1、初始化spark应用
        SparkConf conf = new SparkConf()
                .setAppName("Persist")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 2、加载文件
        JavaRDD<String> lines = sc.textFile("E:\\goku\\spark-basic-code\\src\\main\\files\\word.txt");
        lines.cache();

        // 3、测试缓存效果
        long beginTime = System.currentTimeMillis();

        long count = lines.count();
        System.out.println(count);

        long endTime = System.currentTimeMillis();
        System.out.println("cost " + (endTime - beginTime) + " milliseconds.");

        beginTime = System.currentTimeMillis();

        count = lines.count();
        System.out.println(count);

        endTime = System.currentTimeMillis();
        System.out.println("cost " + (endTime - beginTime) + " milliseconds.");

        // 4、关闭spark应用
        sc.close();

    }

}
