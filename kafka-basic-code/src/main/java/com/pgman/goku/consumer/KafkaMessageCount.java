package com.pgman.goku.consumer;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class KafkaMessageCount {

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        Properties props = new Properties();
        props.put("bootstrap.servers", "test.server:9092");
        props.put("group.id", "goku-test000000");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("api_topic"));

        try {
            int total = 0;
            int num = 0;
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(10);
                for (ConsumerRecord<String, String> record : records) {

                    String json = record.value();

                    if (!(null == json || "".equals(json.toString()))) {

                        JSONObject jsonObject = JSONObject.parseObject(json);

                        if (jsonObject.getString("type").equals("uv_num")) {

                            num++;

                            JSONArray jsonArray = JSONArray.parseArray(jsonObject.getString("data"));

                            int dataSize = jsonArray.size();

                            total = total + dataSize;

                            System.out.println(total + "    " + num);
                        }

                    }

                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }
}

