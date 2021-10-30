package com.jason.mapreduce.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author WangChenHol
 * @date 2021-10-25 16:32
 **/
public class JobUtil {

    public static Job getInstance(Class<?> driverClass, Class<? extends Mapper<?, ?, ?, ?>> mapperClass, Class<? extends Reducer<?, ?, ?, ?>> reduceClass) throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(driverClass);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reduceClass);
        return job;
    }

    public static void setKeyValue(Job job, Class<?> mapOutKey, Class<?> mapOutValue, Class<?> outKey, Class<?> outValue) {
        job.setMapOutputKeyClass(mapOutKey);
        job.setMapOutputValueClass(mapOutValue);
        job.setOutputKeyClass(outKey);
        job.setOutputValueClass(outValue);
    }

    public static void setPath(Job job, String inputPath, String outputPath) throws IOException {
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
    }
}
