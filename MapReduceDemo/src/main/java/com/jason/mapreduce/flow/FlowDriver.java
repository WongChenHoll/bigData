package com.jason.mapreduce.flow;

import com.jason.mapreduce.util.JobUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Hadoop序列化，主要是将一个服务器上的文件移动到另一个服务器上。
 *
 * @author WangChenHol
 * @date 2021-10-26 16:41
 **/
public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = JobUtil.getInstance(FlowDriver.class, FlowMapper.class, FlowReduce.class);
        JobUtil.setKeyValue(job, Text.class, FlowBean.class, Text.class, FlowBean.class);
        JobUtil.setPath(job, "D:\\data\\test-data\\input\\phone", "D:\\data\\test-data\\output\\phone01");

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
