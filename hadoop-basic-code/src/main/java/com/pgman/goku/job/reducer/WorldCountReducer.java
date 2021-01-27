package com.pgman.goku.job.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * WorldCount reducer类开发
 *
 * reducer接收类型：一个字符串类型的key、一个可迭代的数据集；即同一个key的一组value。
 */
public class WorldCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outputValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        outputValue.set(sum);

        context.write(key, outputValue);

    }
}
