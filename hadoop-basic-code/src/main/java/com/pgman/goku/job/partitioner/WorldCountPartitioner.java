package com.pgman.goku.job.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WorldCountPartitioner extends Partitioner<Text, IntWritable> {

    /**
     *
     * @param text
     * @param intWritable
     * @param partitionId
     * @return
     */
    @Override
    public int getPartition(Text text, IntWritable intWritable, int partitionId) {

        int partitionKey = Math.abs(text.toString().hashCode()) / 8;

        return partitionKey;
    }

}
