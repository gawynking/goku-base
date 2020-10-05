package com.pgman.goku.streaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

import scala.Tuple2;

/**
 * 
 * Flume被设计为可以在agent之间推送数据，而不一定是从agent将数据传输到sink中。在这种方式下，Spark
 * Streaming需要启动一个作为Avro Agent的Receiver，来让 flume可以推送数据过来。下面是我们的整合步骤：
 * 
 * 前提需要
 * 
 * 选择一台机器：
 * 
 * 1、Spark Streaming与Flume都可以在这台机器上启动，Spark的其中一个Worker必须运行在这台机器上面
 * 2、Flume可以将数据推送到这台机器上的某个端口
 * 
 * 由于flume的push模型，Spark Streaming必须先启动起来，Receiver要被调度起来并且监听本地某个端口，来让flume推送数据。
 * 
 * 配置flume
 * 
 * 在flume-conf.properties文件中，配置flume的sink是将数据推送到其他的agent中
 * 
 * #agent1表示代理名称
 * 
 * agent1.sources=source1 
 * agent1.sinks=sink1
 * agent1.channels=channel1 
 * 
 * #配置source1 
 * 
 * agent1.sources.source1.type=spooldir
 * agent1.sources.source1.spoolDir=/opt/flume_logs
 * agent1.sources.source1.channels=channel1 
 * agent1.sources.source1.fileHeader =
 * false agent1.sources.source1.interceptors = i1
 * agent1.sources.source1.interceptors.i1.type = timestamp 
 * 
 * #配置channel1
 * agent1.channels.channel1.type=file
 * agent1.channels.channel1.checkpointDir=/opt/flume_logs_tmp_cp
 * agent1.channels.channel1.dataDirs=/opt/flume_logs_tmp 
 * 
 * #配置sink1
 * #agent1.sinks.sink1.type=hdfs
 * #agent1.sinks.sink1.hdfs.path=hdfs://chavin.king:8020/flume_logs
 * #agent1.sinks.sink1.hdfs.fileType=DataStream
 * #agent1.sinks.sink1.hdfs.writeFormat=TEXT
 * #agent1.sinks.sink1.hdfs.rollInterval=1 #agent1.sinks.sink1.channel=channel1
 * #agent1.sinks.sink1.hdfs.filePrefix=%Y-%m-%d
 * 
 * agent1.sinks.sink1.type = avro 
 * agent1.sinks.sink1.channel = channel1
 * agent1.sinks.sink1.hostname = chavin.king 
 * agent1.sinks.sink1.port = 8888
 * 
 * 配置spark streaming
 * 
 * 在我们的spark工程的pom.xml中加入spark streaming整合flume的依赖
 * 
 * groupId = org.apache.spark artifactId = spark-streaming-flume_2.10 version =
 * 1.5.0
 * 
 * 在代码中使用整合flume的方式创建输入DStream
 * 
 * import org.apache.spark.streaming.flume.*;
 * 
 * JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
 * FlumeUtils.createStream(streamingContext, [chosen machine's hostname],
 * [chosen port]);
 * 
 * 这里有一点需要注意的是，这里监听的hostname，必须与cluster manager（比如Standalone Master、YARN
 * ResourceManager）是同一台机器，这样cluster manager 才能匹配到正确的机器，并将receiver调度在正确的机器上运行。
 * 
 * 部署spark streaming应用
 * 
 * 打包工程为一个jar包，使用spark-submit来提交作业
 * 
 * 启动flume agent
 * 
 * flume-ng agent -n agent1 -c conf -f /usr/local/flume/conf/flume-conf.properties -Dflume.root.logger=DEBUG,console
 * 
 * 什么时候我们应该用Spark Streaming整合Kafka去用，做实时计算？ 什么使用应该整合flume？
 * 
 * 看你的实时数据流的产出频率 1、如果你的实时数据流产出特别频繁，比如说一秒钟10w条，那就必须是kafka，分布式的消息缓存中间件，可以承受超高并发
 * 2、如果你的实时数据流产出频率不固定，比如有的时候是1秒10w，有的时候是1个小时才10w，可以选择将数据用nginx日志来表示，每隔一段时间将日志文件
 * 放到flume监控的目录中，然后呢，spark streaming来计算
 * 
 * @author ChavinKing
 *
 */
public class FlumePushWordCount {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// Create a local StreamingContext with two working thread and batch
		// interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("FlumePushWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

		JavaReceiverInputDStream<SparkFlumeEvent> linesDStream = FlumeUtils.createStream(jssc, "chavin.king", 8888);
		
		JavaDStream<String> wordsDStream = linesDStream.flatMap(new FlatMapFunction<SparkFlumeEvent, String>() {
			public Iterator<String> call(SparkFlumeEvent t) throws Exception {
				// TODO Auto-generated method stub
				String line = new String(t.event().getBody().array());  
				return Arrays.asList(line.split(" ")).iterator();   
			}
		});
		
		JavaPairDStream<String, Integer> pairsDStream = wordsDStream.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(t,1);
			}
		});
		
		JavaPairDStream<String, Integer> wordCountsPairDStream = pairsDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});
		
		wordCountsPairDStream.print();		

		jssc.start(); // Start the computation
		jssc.awaitTermination();
		jssc.close();

	}

}
