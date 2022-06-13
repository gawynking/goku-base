package com.pgman.goku.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.net.URL;


public class BatchWordCount {

	public static void main(String[] args) throws Exception {


		// 第一步：使用dataset API ExecutionEnvironment获取运行环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		URL resource = BatchWordCount.class.getResource("/batchWords.txt");
		URL outPath = BatchWordCount.class.getResource("/wordCount.txt");

		// 第二步：指定輸入数据源
		DataSource<String> text = env.readTextFile(resource.getPath());

		//第三步：DataSet执行计算逻辑
		DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
		
		// 设置输出文件格式及并行度
		counts.print();

	}

	// 將flatmap算子计算逻辑单独提取出来
	public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			String[] tokens = value.toLowerCase().split("\\s");
			for (String token : tokens) {
				out.collect(new Tuple2<String, Integer>(token, 1));
			}
		}
	}
}
