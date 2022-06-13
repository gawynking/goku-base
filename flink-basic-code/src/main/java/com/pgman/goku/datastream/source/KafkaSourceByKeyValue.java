package com.pgman.goku.datastream.source;//package com.pgman.goku.datastream.source;
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.typeutils.TupleTypeInfo;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//
//import java.util.Properties;
//
//public class KafkaSourceByKeyValue {
//
//    public static void main(String[] args) throws Exception {
//
//        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");
//
//        String topicName = "pgman";
//        String groupId = "flink-pgman-test-kafka-string";
//
//        // 1 初始化flink环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 定义kafka参数
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "test.server:9092");
//        properties.put("group.id", groupId);
//        properties.put("auto.offset.reset", "latest"); // 从最新开始消费
//        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        // 注册kafka数据源
//        DataStreamSource<Tuple2<String, String>> kafkaStream = env.addSource(new FlinkKafkaConsumer011<Tuple2<String, String>>(
//                topicName,
//                new KafkaDeserializationSchema<Tuple2<String, String>>() {
//                    // 流是否结束
//                    @Override
//                    public boolean isEndOfStream(Tuple2<String, String> str) {
//                        return false;
//                    }
//
//                    // 定义业务逻辑
//                    @Override
//                    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
//                        if(consumerRecord != null){
//                            String key = null;
//                            String value = null;
//                            if(consumerRecord.key() != null){
//                                key = new String(consumerRecord.key(),"UTF-8");
//                            }
//                            if(consumerRecord.value() != null){
//                                key = new String(consumerRecord.value(),"UTF-8");
//                            }
//                            return new Tuple2<>(key,value);
//                        }else {
//                            return new Tuple2<>(null,null);
//                        }
//                    }
//
//                    // 返回schema
//                    @Override
//                    public TypeInformation<Tuple2<String, String>> getProducedType() {
//                        return null;
//                    }
//                }, properties));
//
//
//        kafkaStream.print();
//
//        env.execute("KafkaSourceByKeyValue");
//
//    }
//
//
//}
