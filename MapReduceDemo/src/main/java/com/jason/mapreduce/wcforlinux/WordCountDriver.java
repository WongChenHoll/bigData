package com.jason.mapreduce.wcforlinux;

import com.jason.mapreduce.util.JobUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * 单词统计Driver类
 *
 * @author WangChenHol
 * @date 2021-10-22 16:10
 **/
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = JobUtil.getInstance(WordCountDriver.class, WordCountMapper.class, WordCountReduce.class);
        JobUtil.setKeyValue(job, Text.class, IntWritable.class, Text.class, IntWritable.class);
        JobUtil.setPath(job, args[0], args[1]);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
