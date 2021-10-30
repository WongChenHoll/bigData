package com.jason.mapreduce.testcombine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 单词统计Driver类
 *
 * @author WangChenHol
 * @date 2021-10-22 16:10
 **/
public class CombineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(CombineDriver.class);
        job.setMapperClass(CombineMapper.class);
        job.setReducerClass(CombineReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024); //虚拟存储切片最大值设置4m

        FileInputFormat.setInputPaths(job, new Path("D:\\data\\test-data\\input\\combine"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\data\\test-data\\output\\combine"));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
