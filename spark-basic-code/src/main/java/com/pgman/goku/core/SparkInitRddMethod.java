package com.pgman.goku.core;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class SparkInitRddMethod {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        parallelizedCollections();
        externalFile();

    }

    /**
     * 并行集合方式初始化RDD
     */
    public static void parallelizedCollections() {

        SparkConf conf = new SparkConf().setAppName("parallelizedCollections").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> dataRdd = sc.parallelize(data);

        Integer reduceRdd = dataRdd.reduce(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {
                // TODO Auto-generated method stub
                return v1 + v2;
            }
        });

        System.out.println(reduceRdd);

        sc.close();

    }

    /**
     * 外部文件方式初始化RDD.
     * <p>
     * 1、SparkContext.wholeTextFiles()方法，可以针对一个目录中的大量小文件，返回<filename, fileContent>组成的pair，作为一个PairRDD，而不是普通的RDD。普通的textFile()返回的RDD中，每个元素就是文件中的一行文本。
     * 2、SparkContext.sequenceFile[K, V]()方法，可以针对SequenceFile创建RDD，K和V泛型类型就是SequenceFile的key和value的类型。K和V要求必须是Hadoop的序列化类型，比如IntWritable、Text等。
     * 3、SparkContext.hadoopRDD()方法，对于Hadoop的自定义输入类型，可以创建RDD。该方法接收JobConf、InputFormatClass、Key和Value的Class。
     * 4、SparkContext.objectFile()方法，可以针对之前调用RDD.saveAsObjectFile()创建的对象序列化的文件，反序列化文件中的数据，并创建一个RDD。
     */
    public static void externalFile() {

        SparkConf conf = new SparkConf().setAppName("externalDatasets").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("E:\\goku\\spark-basic-code\\src\\main\\files\\word.txt");
        System.out.println(lines.count());

        sc.close();

    }
}
