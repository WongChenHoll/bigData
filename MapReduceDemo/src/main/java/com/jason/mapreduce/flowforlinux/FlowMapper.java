package com.jason.mapreduce.flowforlinux;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author WangChenHol
 * @date 2021-10-25 17:24
 **/
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private FlowBean flow = new FlowBean();
    private Text outK = new Text();

//    1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
//    2	13846544121	192.196.100.2			        264	    0	    200

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");

        flow.setUpFlow(Long.valueOf(words[words.length - 3]));
        flow.setDownFlow(Long.valueOf(words[words.length - 2]));
        flow.setCountFlow();
        outK.set(words[1]);
        context.write(outK, flow);
    }
}
