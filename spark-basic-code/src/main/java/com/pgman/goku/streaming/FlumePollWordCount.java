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
 	* 除了让flume将数据推送到spark streaming，还有一种方式，可以运行一个自定义的flume sink
 	* 1、Flume推送数据到sink中，然后数据缓存在sink中
 	* 2、spark streaming用一个可靠的flume receiver以及事务机制从sink中拉取数据
 	* 
 	* 前提条件：
 	* 1、选择一台可以在flume agent中运行自定义sink的机器
 	* 2、将flume的数据管道流配置为将数据传送到那个sink中
 	* 3、spark streaming所在的机器可以从那个sink中拉取数据
 	* 
 	* 配置flume
 	* 
 	* 1、加入sink jars，将以下jar加入flume的classpath中
 	* 
 	* groupId = org.apache.spark
 	* artifactId = spark-streaming-flume-sink_2.10
 	* version = 2.1.0
 	* 
 	* groupId = org.scala-lang
 	* artifactId = scala-library
 	* version = 2.11.8
 	* 
 	* groupId = org.apache.commons
 	* artifactId = commons-lang3
 	* version = 3.3.2
 	* 
 	* 注意：spark-streaming-flume-sink_2.10文件需要cp到flume/lib目录下，否则会报错：org.apache.flume.FlumeException: Unable to load sink type: org.apache.spark.streaming.flume.sink.SparkSink, class: org.apache.spark.streaming.flume.sink.SparkSink
 	* 
 	* 2、修改配置文件
 	* 
 	* 
 	* #agent1表示代理名称
 	* agent1.sources=source1
 	* agent1.sinks=sink1
 	* agent1.channels=channel1
 	* 
 	* #配置source1
 	* 
 	* agent1.sources.source1.type=spooldir
 	* agent1.sources.source1.spoolDir=/opt/flume_logs
 	* agent1.sources.source1.channels=channel1
 	* agent1.sources.source1.fileHeader = false
 	* agent1.sources.source1.interceptors = i1
 	* agent1.sources.source1.interceptors.i1.type = timestamp
 	* 
 	* #配置channel1
 	* 
 	* agent1.channels.channel1.type=file
 	* agent1.channels.channel1.checkpointDir=/opt/flume_logs_tmp_cp
 	* agent1.channels.channel1.dataDirs=/opt/flume_logs_tmp
 	* 
 	* #配置sink1
 	* 
 	* #agent1.sinks.sink1.type=hdfs
 	* #agent1.sinks.sink1.hdfs.path=hdfs://chavin.king:8020/flume_logs
 	* #agent1.sinks.sink1.hdfs.fileType=DataStream
 	* #agent1.sinks.sink1.hdfs.writeFormat=TEXT
 	* #agent1.sinks.sink1.hdfs.rollInterval=1
 	* #agent1.sinks.sink1.channel=channel1
 	* #agent1.sinks.sink1.hdfs.filePrefix=%Y-%m-%d
 	* 
 	* agent1.sinks.sink1.type = org.apache.spark.streaming.flume.sink.SparkSink
 	* agent1.sinks.sink1.hostname = chavin.king
 	* agent1.sinks.sink1.port = 8888
 	* agent1.sinks.sink1.channel = channel1
 	* 
 	* 配置spark streaming
 	* 
 	* import org.apache.spark.streaming.flume.*;
 	* 
 	* JavaReceiverInputDStream<SparkFlumeEvent>flumeStream =
 	* 	FlumeUtils.createPollingStream(streamingContext, [sink machine hostname], [sink port]);
 	* 	
 	* 一定要先启动flume，再启动spark streaming
 	* 
 	* flume-ng agent -n agent1 -c conf -f /usr/local/flume/conf/flume-conf.properties -Dflume.root.logger=DEBUG,console
 	* 
    * @author ChavinKing
    *
    */
public class FlumePollWordCount {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// Create a local StreamingContext with two working thread and batch
		// interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("FlumePollWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

		JavaReceiverInputDStream<SparkFlumeEvent> linesDStream = FlumeUtils.createPollingStream(jssc, "chavin.king",
				8888);

		JavaDStream<String> wordsDStream = linesDStream.flatMap(new FlatMapFunction<SparkFlumeEvent, String>() {
			public Iterator<String> call(SparkFlumeEvent t) throws Exception {
				// TODO Auto-generated method stub
				String line = new String(t.event().getBody().array());
				return Arrays.asList(line.split(" ")).iterator();
			}
		});

		JavaPairDStream<String, Integer> pairsDStream = wordsDStream
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, Integer>(t, 1);
					}
				});

		JavaPairDStream<String, Integer> wordCountsPairDStream = pairsDStream
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

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
