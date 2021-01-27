package com.pgman.goku.job.driver;


import com.pgman.goku.job.mapper.WorldCountMapper;
import com.pgman.goku.job.partitioner.WorldCountPartitioner;
import com.pgman.goku.job.reducer.WorldCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * GroupingComparator : 组比较器
 *
 */
public class WorldCount {

    /**
     * 驱动任务，用于组装map和reduce任务
     *
     * @return
     */
    public boolean run(String[] args) throws Exception {

//        创建配置对象
        Configuration configuration = new Configuration();

//        创建job对象
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());

//        设置运行job的类
        job.setJarByClass(WorldCount.class);

//        设置输入文件路径
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);

//        设置输出文件路径
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

//        设置Mapper信息
        job.setMapperClass(WorldCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

//        设置分区类 ,同时指定分区数
        job.setPartitionerClass(WorldCountPartitioner.class);
        job.setNumReduceTasks(8);

//        设置Reducer信息
        job.setReducerClass(WorldCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        submit job
        return job.waitForCompletion(true);

    }


    /**
     * 单元测试
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        args = new String[]{
                "",
                ""
        };

        boolean flag = new WorldCount().run(args);

        if (!flag) {
            System.exit(1);
        }

    }

}
