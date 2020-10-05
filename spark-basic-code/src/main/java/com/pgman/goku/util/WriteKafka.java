package com.pgman.goku.util;

import java.sql.*;
import java.util.*;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class WriteKafka {

    public static Logger logger = LoggerFactory.getLogger(WriteKafka.class);

    public static void main(String[] args) {

        int taskID = Integer.parseInt(args[0]);

        String serverAddr = null;
        String topicName = null;
        String sqlStr = null;
        String typeTag = null;

        Map<String, String> map = JDBCmysqlmethod(taskID);

        serverAddr = map.get("serverAddr");
        topicName = map.get("topicName");
        typeTag = map.get("typeTag");
        sqlStr = map.get("sqlStr");

        logger.info("this serverAddr is {}", serverAddr);
        logger.info("this topicName is {}", topicName);
        logger.info("this typeTag is {}", typeTag);
        logger.info("this sqlStr is {}", sqlStr);

        sendKafka(serverAddr, topicName, typeTag, sqlStr);

    }


    public static void sendKafka(String serverAddr, String topicName, String typeTag, String sqlStr) {

        SparkSession spark = SparkSession
                .builder()
//				  .master("local")
                .appName("intelligent-touch")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
                .enableHiveSupport()
                .getOrCreate();


//		生成测试文件
//		Dataset<Row> df = spark.read().json("files//people.json");
//		df.createOrReplaceTempView("people");

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        Accumulator<Integer> sumkafkaAfter = jsc.accumulator(0);

        try {

            String querySql = sqlStr;
            logger.info("this query sql is {}", querySql);
            Dataset<Row> resultSet = spark.sql(querySql);

            logger.info("return dataset<row> {1111111111111111111111111111111111111111111111111111111111111111111111}");

            resultSet.foreachPartition(new ForeachPartitionFunction<Row>() {

                private static final long serialVersionUID = 1L;

                @Override
                public void call(Iterator<Row> t) throws Exception {

                    logger.info("start foreachPartition {1111111111111111111111111111111111111111111111111111111111111111111111}");

                    KafkaProducer eventKafkaProducer;
                    Properties properties = new Properties();
                    properties.setProperty("bootstrap.servers", serverAddr);
                    properties.setProperty("acks", "all");
                    properties.setProperty("retries", "2147483647");
                    properties.setProperty("batch.size", "16384");
                    properties.setProperty("linger.ms", "1");
                    properties.setProperty("batch.memory", "33554432");
                    properties.setProperty("block.on.buffer.full", "true");
                    properties.setProperty("max.in.flight.requests.per.connection", "1");
                    properties.setProperty("unclean.leader.election.enable", "false");
                    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    properties.setProperty("producer.topic", topicName);
                    eventKafkaProducer = new KafkaProducer(properties);


                    Map<String, Integer> numMap = Maps.newHashMap();
                    numMap.put("sendNum", 0);
                    numMap.put("sendFaildNum", 0);


                    List<Map<String, Object>> searchListData = Lists.newArrayList();
                    Map<String, Object> searchMapData;
                    Map<String, Object> resultMapData;


                    int shouldNum = 0;
                    while (t.hasNext()) {

                        logger.info("start hasNext function 1111111111111111111111111111111111111111111111111111111111111111111111");

                        searchMapData = Maps.newHashMap();
                        shouldNum++;
                        Row row = t.next();
                        StructType schema = row.schema();
                        String[] schemas = schema.fieldNames();

                        for (int i = 0; i < row.length(); i++) {

                            String key = schemas[i];
                            Object value = row.get(i);

                            searchMapData.put(key, value);
                        }

                        searchListData.add(searchMapData);

                        if (shouldNum % 1 == 0) {

                            resultMapData = Maps.newHashMap();
                            resultMapData.put("type", typeTag);
                            resultMapData.put("data", searchListData);

                            sendMessage(resultMapData, eventKafkaProducer, sumkafkaAfter, numMap, topicName);
                            searchListData.clear();

                        }

                    }

                    resultMapData = Maps.newHashMap();
                    resultMapData.put("type", typeTag);
                    resultMapData.put("data", searchListData);

                    sendMessage(resultMapData, eventKafkaProducer, sumkafkaAfter, numMap, topicName);
                    searchListData.clear();

                }

            });

        } catch (Exception e) {

        }

        logger.info("kafka    发送总量后为 {}", sumkafkaAfter.value());
        jsc.close();
        spark.close();
    }

    @SuppressWarnings("unchecked")
    private static void sendMessage(Map<String, Object> map, KafkaProducer eventKafkaProducer,
                                    Accumulator<Integer> sumkafkaAfter, Map<String, Integer> numMap, String topicName) {

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, JSONObject.toJSONString(map));

        try {

            logger.info("start hasNext send 1111111111111111111111111111111111111111111111111111111111111111111111");

            eventKafkaProducer.send(record).get();

//			eventKafkaProducer.send(record, new Callback() {
//
//				@Override
//				public void onCompletion(RecordMetadata metadata, Exception exception) {
//					if (exception != null) {
//						Integer num = numMap.get("sendFaildNum");
//						num += 1;
//						numMap.put("sendFaildNum", num);
//						logger.info(" kafak 发送失败 {} ", exception.getMessage());
//						logger.info(" 失败后的metadata {} ", metadata);
//
//					} else {
//						sumkafkaAfter.add(1);
//						Integer num = numMap.get("sendNum");
//						num += 1;
//						numMap.put("sendNum", num);
//						// logger.info(" kafak 发送成功 ");
//					}
//				}
//			});

        } catch (Exception e) {
            logger.error("push message to kafka error .. {}", e);
        }
    }

    public static Map<String, String> JDBCmysqlmethod(int id) {

        Connection conn = null;
        Statement statement = null;
        ResultSet resultset = null;

        Map map = new HashMap<String, String>();

        String sql = "select " +
                "t1.task_id," +
                "t2.server_addr," +
                "t1.topic_name," +
                "t1.sql_str," +
                "t1.type_tag " +

                "from etl_kafka_task t1 " +
                "join etl_kafka_server t2 on t1.server_id = t2.server_id " +
                "where t1.valid_flag = 'Y' " +
                "and t2.valid_flag = 'Y' " +
                "and t1.task_id = ? ";

        try {
            Class.forName("com.mysql.jdbc.Driver");// 加载驱动
            conn = DriverManager.getConnection("jdbc:mysql://192.168.10.11:3306/julive_dw", "root", "QemENw>O0.Z");// 连接数据库
            PreparedStatement pstmt = conn.prepareStatement(sql);

            pstmt.setInt(1, id);

            resultset = pstmt.executeQuery();

            while (resultset.next()) {
                map.put("serverAddr", resultset.getString("server_addr"));
                map.put("topicName", resultset.getString("topic_name"));
                map.put("sqlStr", resultset.getString("sql_str"));
                map.put("typeTag", resultset.getString("type_tag"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

}