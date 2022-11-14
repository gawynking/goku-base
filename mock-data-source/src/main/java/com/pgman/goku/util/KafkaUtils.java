package com.pgman.goku.util;

import com.pgman.goku.config.ConfigurationManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaUtils {

    public static Logger logger = LoggerFactory.getLogger(KafkaUtils.class);


    private static KafkaUtils instance = null;

    public static KafkaUtils getInstance() {
        if (instance == null) {
            synchronized (KafkaUtils.class) {
                if (instance == null) {
                    instance = new KafkaUtils();
                }
            }
        }
        return instance;
    }

    private Producer<String, String> producer = null;

    private KafkaUtils(){

        String brokerList = ConfigurationManager.getString("broker.list");
        Properties properties = new Properties();

        properties.put("bootstrap.servers", brokerList);
        properties.put("acks", "all");
        properties.put("retries", 3);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer(properties);

    }


    /**
     * 发送消息
     *
     * @param topic
     * @param message
     */
    public void send(String topic,String message){
        producer.send(new ProducerRecord<String, String>(topic, message));
    }

    /**
     * 关闭kafka
     */
    public void close(){
        if(null != producer){
            producer.close();
        }
    }


}
