import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Test {

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
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);

        for (int i = 0; i < 1000000000; i++) {

            int j = i%5;
            producer.send(new ProducerRecord<String, String>("spark-test", Integer.toString(i), "pgman" + Integer.toString(j) + " " + "pgman" + Integer.toString(i)));
            Thread.sleep(1000);
        }
        producer.close();

    }

}
