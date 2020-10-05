package com.pgman.goku.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

/**
 * 累加变量：它们可用于实现计数器（如MapReduce中的计数器）或求和
 * 可以通过分别调用SparkContext.longAccumulator()或SparkContext.doubleAccumulator() 累加Long或Double类型的值来创建数字累加器。
 * 然后，可以使用该add方法将在集群上运行的任务添加到其中。但是，他们无法读取其值。只有驱动程序可以使用其value方法读取累加器的值。
 *
 * @author Administrator
 *
 */
public class AccumulatorVariable {

	public static void main(String[] args) {

		// 0、设置hadoop环境，没有必要注释掉这行
		System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

		// 1、经典方式
		accumulatorOld();

		// 2、调用新api
		accumulatorNew();

	}

	/**
	 * 旧api版本
	 */
	public static void accumulatorOld() {

		// 1、初始化spark应用
		SparkConf conf = new SparkConf()
				.setAppName("Accumulator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);


		// 2、创建Accumulator变量，需要调用SparkContext.accumulator()方法
		final Accumulator<Integer> sum = sc.accumulator(0);

		// 3、创建测试rdd
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);

		// 4、将rdd中数据值累加到sum变量
		numbers.foreach(new VoidFunction<Integer>() {

			private static final long serialVersionUID = 1L;


			public void call(Integer t) throws Exception {
				// 然后在函数内部，就可以对Accumulator变量，调用add()方法，累加值
				sum.add(t);
			}

		});

		// 5、在driver程序中，可以调用Accumulator的value()方法，获取其值
		System.out.println("accumulatorOld()值为：" + sum.value());

		// 6、关闭spark
		sc.close();
	}

	/**
	 * 可以通过分别调用SparkContext.longAccumulator()或SparkContext.doubleAccumulator() 累加Long或Double类型的值来创建数字累加器。
	 * 然后，可以使用该add方法将在集群上运行的任务添加到其中。但是，他们无法读取其值。只有驱动程序可以使用其value方法读取累加器的值。
	 */
	public static void accumulatorNew(){

		// 1、初始化spark应用
		SparkConf conf = new SparkConf()
				.setAppName("Accumulator")
				.setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		// 2、SparkContext.longAccumulator(),初始值为0
		LongAccumulator longAccumulator = jsc.sc().longAccumulator();

		// 3、创建测试rdd
		JavaRDD<Integer> testRdd = jsc.parallelize(Arrays.asList(1,2,3,4,5));

		// 4、使用累加器求和
		testRdd.foreach(new VoidFunction<Integer>() {
			@Override
			public void call(Integer value) throws Exception {
				longAccumulator.add(value);
			}
		});

		// 5、driver端取出累积器结果值
		System.out.println("accumulatorNew()值为：" + longAccumulator.value());

		// 6、关闭spark
		jsc.close();

	}

}
