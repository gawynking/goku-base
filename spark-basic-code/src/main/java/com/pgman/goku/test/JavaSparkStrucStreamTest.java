package com.pgman.goku.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

public class JavaSparkStrucStreamTest {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaStructuredKafkaWordCount")
                .getOrCreate();


        // Read all the csv files written atomically in a directory
        StructType userSchema = new StructType().add("name", "string");
        Dataset<Row> csvDF = spark
                .readStream()
//                .option("sep", ";")
                .schema(userSchema)      // Specify schema of the csv files
                .format("csv")
                .load("E:\\text\\test");

//                .csv("E:\\text\\test");    // Equivalent to format("csv").load("/path/to/directory")

        // Start running the query that prints the running counts to the console
        StreamingQuery query = csvDF.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();
    }

}
