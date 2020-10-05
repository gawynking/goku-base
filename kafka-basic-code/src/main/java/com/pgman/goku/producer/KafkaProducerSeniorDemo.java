package com.pgman.goku.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerSeniorDemo {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Properties props = new Properties();

		props.put("bootstrap.servers", "test.server:9092");
		props.put("acks", "1");
		//props.put("acks", "all");
		props.put("retries", 3);
		props.put("batch.size", 16384);
		props.put("linger.ms", 10);
		props.put("buffer.memory", 33554432);
		props.put("max.block.ms", 3000);
		props.put("max.request.size", 10485760);
		props.put("request.timeout.ms", 60000);
		props.put("block.on.buffer.full", "true");
		props.put("compressiont.typ", "snappy");

		Serializer<String> keySerializer =new StringSerializer();
		Serializer<String> valueSerializer =new StringSerializer();

		Producer<String, String> producer = new KafkaProducer(props,keySerializer,valueSerializer);

		for (int i = 0; i < 100; i++) {
			// 同步发送 
			// producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i))).get();
			
			// 带回调函数的生产者程序
			producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)),new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// TODO Auto-generated method stub
					
					if (exception == null) {
						// 消息发送成功逻辑
						System.out.println("消息发送成功");
					} else {
						if (exception instanceof RetriableException) {
							// 处理可重试瞬时异常逻辑
							System.out.println("处理可重试瞬时异常");
						} else {
							// 处理不可重试异常逻辑
							System.out.println("处理不可重试异常");
						}
					}
						
				}
			});
			
		}
		producer.close();

	}

}
