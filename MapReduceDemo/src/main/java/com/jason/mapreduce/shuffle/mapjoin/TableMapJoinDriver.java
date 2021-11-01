package com.jason.mapreduce.shuffle.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 处理多个文件相关联的情况。采用map join 方式。
 * 输出为一个文件
 *
 * @author WangChenHol
 * @date 2021-11-1 17:41
 **/
public class TableMapJoinDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(TableMapJoinDriver.class);
        job.setMapperClass(TableMapJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5 设置最终输出KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 添加缓存文件
        job.addCacheFile(new URI("file:///D:/data/test-data/input/hadoop/join/product.txt"));
        job.setNumReduceTasks(0); // 将reduceTask设为0，也就是程序不走reducer

        FileInputFormat.setInputPaths(job, new Path("D:\\data\\test-data\\input\\hadoop\\join\\mapjoin"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\data\\test-data\\output\\hadoop\\mapjoin"));
        // 7 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);



    }
}
