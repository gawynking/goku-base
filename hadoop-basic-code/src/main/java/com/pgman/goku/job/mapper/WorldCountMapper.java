package com.pgman.goku.job.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * WorldCount map类开发
 *
 * MapReduce框架每读到一行数据，就会调用一次这个map方法。
 */
public class WorldCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text mapOutputKey = new Text();
    private IntWritable mapOutputValue = new IntWritable(1);

    /**
     * 覆写map方法
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);

        String lineValue = value.toString();
        String[] words = lineValue.split("\\s");
        for(String word :words){
            mapOutputKey.set(word);
            context.write(mapOutputKey,mapOutputValue);
        }

    }

}
