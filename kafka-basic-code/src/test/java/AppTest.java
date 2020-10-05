import java.util.Arrays;
import java.util.Properties;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class AppTest {

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

        String topicName = "api_topic";
        String groupId = "ck-test00200908kkk";

        Properties props = new Properties();
        props.put("bootstrap.servers", "test.server:9092");
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");// 自动提交
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest"); // 从最早开始消费

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {

                    String json = record.value();

                    if (!(null == json || "".equals(json.toString()))) {

                        JSONObject jsonObject = JSONObject.parseObject(json);

                        System.out.println(jsonObject);

                        if (jsonObject.getString("type").equals("exposure")) {

//                            System.out.println(jsonObject.get("type"));
//                            System.out.println("------------------------------------------------");
//                            System.out.println(jsonObject.get("data"));

                            JSONObject data = JSONObject.parseObject(jsonObject.get("data").toString());

                            System.out.print(data.get("flash_type") + "  ");
                            System.out.print(data.get("content_id") + "  ");
                            System.out.print(data.get("click_num") + "  ");
                            System.out.println(data.get("type"));

                        }

                    }

                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }

    }
}
