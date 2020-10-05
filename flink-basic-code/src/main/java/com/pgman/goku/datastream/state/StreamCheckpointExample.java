package com.pgman.goku.datastream.state;


import com.pgman.goku.config.ConfigurationManager;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;


/**
 * 测试checkpoint的状态恢复
 *
 * 检查点故障恢复不是自动的，从检查点恢复数据命令：
 * flink run \
 * -m yarn-cluster \
 * -ynm test_cp \
 * -p 1 \
 * -c com.pgman.goku.datastream.state.StreamCheckpointExample \
 * -s hdfs://cdh01:8020/chavin/cp01/04f4f40f10d360fa15af642413354a66/chk-504/_metadata \
 * /flink/test.jar
 *
 */
public class StreamCheckpointExample {


    public static void main(String[] args) throws Exception{

        // 1 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 启动checkpoint机制
        env.enableCheckpointing(1000);
        env.setStateBackend(new FsStateBackend("hdfs://cdh01:8020/flink/ck02"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 检查点语义
        env.getCheckpointConfig().setCheckpointTimeout(5000); // 检查点超时时间
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100); // 两次检查点间的最小时间间隔，该参数与setMaxConcurrentCheckpoints冲突
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,100)); // 配置重启策略


        // 2 注册数据源
        // 定义kafka参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConfigurationManager.getString("broker.list"));
        properties.put("group.id", "abcdefg-111111");
        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        DataStreamSource<String> jsonText = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        "pgman",
                        new SimpleStringSchema(),
                        properties
                )
        );

        // 3 进行逻辑运算
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = jsonText.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
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
        JobExecutionResult result = env.execute("StreamWordCount");

    }

}
