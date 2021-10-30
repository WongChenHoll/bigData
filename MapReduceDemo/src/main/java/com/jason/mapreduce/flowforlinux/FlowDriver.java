package com.jason.mapreduce.flowforlinux;

import com.jason.mapreduce.util.JobUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * 集群上执行命令：hadoop jar flow.jar com.jason.mapreduce.flowforlinux.FlowDriver /test_input_file/phone /output/phone
 *
 * @author WangChenHol
 * @date 2021-10-26 16:41
 **/
public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = JobUtil.getInstance(FlowDriver.class, FlowMapper.class, FlowReduce.class);
        JobUtil.setKeyValue(job, Text.class, FlowBean.class, Text.class, FlowBean.class);
        JobUtil.setPath(job, args[0], args[1]);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
