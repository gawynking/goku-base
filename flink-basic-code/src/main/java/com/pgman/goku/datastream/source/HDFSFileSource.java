package com.pgman.goku.datastream.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class HDFSFileSource {

    public static void main(String[] args) throws Exception{

        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        // 1 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 注册数据源
        // StreamExecutionEnvironment.readTextFile(String filePath) 可以用来加载本地文件或者hdfs文件
        DataStreamSource<String> stream = env.readTextFile("hdfs://192.168.72.129:8020/workcont/wc.input");

        // 3 进行逻辑运算
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : s.split(" ")) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(0).sum(1);

        // 4 定义sink
        wordCount.print("结果:");

        // 5 启动流任务
        env.execute("HDFSFileSource");


    }
}
