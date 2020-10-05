package com.pgman.goku.dataset.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.log4j.Logger;
import java.util.Properties;

public class AccumulatorExample {


    public static void main(String[] args) {

        // 初始化flink环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSource<Integer> text = env.fromElements(1, 2, 3);


        MapOperator<Integer, Integer> mapDataStream = text.map(new RichMapFunction<Integer, Integer>() {

            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public Integer map(Integer integer) throws Exception {

                this.numLines.add(1);
                return integer * 2;

            }

        });

        mapDataStream.writeAsText("E:\\test\\test1");

        try {

            JobExecutionResult result = env.execute();
            Integer lineCounter = result.getAccumulatorResult("num-lines");
            System.out.println(lineCounter); // 3

        }catch (Exception e){
            e.printStackTrace();
        }


    }


}
