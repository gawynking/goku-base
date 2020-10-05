import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class TestConsumer {

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

//        String topicName = "julive_app_xpt_platform_topic"; // target
        String topicName = "big_dipper_event_test_topic"; // source

        String groupId = "ck-test00200908kkk2d51";

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
//                    System.out.printf("offset=%d, key=%s, value=%s%n", record.offset(), record.key(), record.value());

                    if(record.value().contains("op_type")){
                        System.out.println( record.offset() + "   :    " + record.value());
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
