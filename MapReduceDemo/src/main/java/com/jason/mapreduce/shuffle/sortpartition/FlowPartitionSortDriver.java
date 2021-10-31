package com.jason.mapreduce.shuffle.sortpartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 对文件数据按手机号前三位进行分区，并且对每一个分区进行排序。
 *
 * @author WangChenHol
 * @date 2021-10-31 10:21
 **/
public class FlowPartitionSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowPartitionSortDriver.class);
        job.setMapperClass(FlowPartitionSortMapper.class);
        job.setReducerClass(FlowPartitionSortReducer.class);

        job.setMapOutputKeyClass(FlowPartitionSortBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(FlowPartitionSortBean.class);

        job.setPartitionerClass(FlowSortPartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path("D:\\data\\test-data\\input\\phone"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\data\\test-data\\output\\sortPartition"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
