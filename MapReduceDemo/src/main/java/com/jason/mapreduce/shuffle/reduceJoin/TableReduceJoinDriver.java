package com.jason.mapreduce.shuffle.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 处理多张文件表join的问题，输出为一张表（一个文件）。reduce join 方式
 * 文件在同目录下。
 *
 * @author WangChenHol
 * @date 2021-11-1 17:14
 **/
public class TableReduceJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(TableReduceJoinDriver.class);
        job.setMapperClass(TableReduceJoinMapper.class);
        job.setReducerClass(TableReduceJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\data\\test-data\\input\\hadoop\\join"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\data\\test-data\\output\\hadoop\\reduceJoin"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
