package com.pgman.goku.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * 广播变量：广播变量使程序员可以在每台计算机上保留一个只读变量，而不是将其副本发送到每一个task
 * @author Administrator
 *
 */
public class BroadcastVariable {

	public static void main(String[] args) {

		// 0、设置hadoop环境，没有必要注释掉这行
		System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

		// 1、初始化spark应用
		SparkConf conf = new SparkConf()
				.setAppName("BroadcastVariable") 
				.setMaster("local"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		// 3、在java中，创建共享变量，就是调用SparkContext.broadcast()方法，获取的返回结果是Broadcast<T>类型
		final int factor = 3;
		final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);

		// 4、创建测试rdd
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		
		// 5、让集合中的每个数字，都乘以外部定义的那个factor
		JavaRDD<Integer> multipleNumbers = numbers.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;
			
			public Integer call(Integer v1) throws Exception {
				// 使用共享变量时，调用其value()方法，即可获取其内部封装的值
				int factor = factorBroadcast.value();
				return v1 * factor;
			}
			
		});

		// 6、打印结果
		multipleNumbers.foreach(new VoidFunction<Integer>() {
			
			private static final long serialVersionUID = 1L;
			
			public void call(Integer t) throws Exception {
				System.out.println(t);  
			}
			
		});

		// 7、关闭spark
		sc.close();
	}
	
}
