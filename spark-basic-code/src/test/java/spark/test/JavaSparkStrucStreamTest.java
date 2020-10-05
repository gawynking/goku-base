package java.spark.test;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

public class JavaSparkStrucStreamTest {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        String bootstrapServers = "test.server:9092";
        String subscribeType = "subscribe";
        String topics = "spark-test";

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaStructuredKafkaWordCount")
                .getOrCreate();

        // Create DataSet representing the stream of input lines from kafka
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option(subscribeType, topics)
                .load()
                .selectExpr("CAST(value AS STRING)");
//                .as(Encoders.STRING());

        lines.registerTempTable("words");

        Dataset<Row> result = spark.sql("select value,count(1) from words group by value");

//        // Generate running word count
//        Dataset<Row> wordCounts = lines.flatMap(
//                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
//                Encoders.STRING()).groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = result.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }

}
