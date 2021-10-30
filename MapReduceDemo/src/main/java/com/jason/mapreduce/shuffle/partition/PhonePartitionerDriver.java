package com.jason.mapreduce.shuffle.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 测试MapReduce的分区Partition
 *
 * @author WangChenHol
 * @date 2021-10-29 17:00
 **/
public class PhonePartitionerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 让Job关联Driver、Mapper、Reduce
        job.setJarByClass(PhonePartitionerDriver.class);
        job.setMapperClass(PhonePartitionerMapper.class);
        job.setReducerClass(PhonePartitionerReduce.class);

        // 设置输出的KEY-VALUE类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhonePartitionerFlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhonePartitionerFlowBean.class);

        // 关键步骤：设置分区和ReduceTask数量
        job.setPartitionerClass(PhonePartitioner.class);
        job.setNumReduceTasks(5);

        //设置文件的输入输出目录
        FileInputFormat.setInputPaths(job, new Path("D:\\data\\test-data\\input\\phone"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\data\\test-data\\output\\shuffle\\partitioner\\phone01"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
