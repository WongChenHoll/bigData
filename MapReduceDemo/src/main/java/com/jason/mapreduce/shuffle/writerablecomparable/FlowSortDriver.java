package com.jason.mapreduce.shuffle.writerablecomparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author WangChenHol
 * @date 2021-10-31 10:21
 **/
public class FlowSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowSortDriver.class);
        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(FlowSortBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\data\\test-data\\input\\phone"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\data\\test-data\\output\\sort\\phone"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
