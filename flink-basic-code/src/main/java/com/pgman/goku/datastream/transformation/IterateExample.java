package com.pgman.goku.datastream.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 通过将一个操作符的输出重定向到某个先前的操作符，在流中创建“反馈”循环。这对于定义不断更新模型的算法特别有用。以下代码以流开头并连续应用迭代体。大于0的元素将被发送回反馈通道，其余元素将向下游转发。
 *
 */
public class IterateExample {


    public static void main(String[] args){

        // 定义流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 注册dataStream对象
        DataStream<Long> someIntegers = env.generateSequence(1L, 10L);

        // 获取迭代器流对象
        IterativeStream<Long> iteration = someIntegers.iterate();

        // 迭代計算
        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1 ;
            }
        });

        DataStream<Long> feedback = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });

        iteration.closeWith(feedback);

        // 滿足條件后觸發運算
        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value <= 0);
            }
        });

        SingleOutputStreamOperator<String> passDown = lessThanZero.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "返回标识";
            }
        });

        passDown.print("passDown");

        try {
            env.execute("IterationExample");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
