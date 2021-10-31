package com.jason.mapreduce.shuffle.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * 自定义RecordWriter，使包含Thread的日志输出到thread.log，其他的输出到other.log文件。
 *
 * @author WangChenHol
 * @date 2021-10-31 23:01
 **/
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream threadLog;
    private FSDataOutputStream otherLog;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            threadLog = fileSystem.create(new Path("D:\\data\\test-data\\output\\log\\thread.log"));
            otherLog = fileSystem.create(new Path("D:\\data\\test-data\\output\\log\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        if (log.contains("Thread")) {
            threadLog.writeBytes(log + "\n");
        } else {
            otherLog.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) {
        IOUtils.closeStream(threadLog);
        IOUtils.closeStream(otherLog);
    }
}
