package com.jason.mapreduce.shuffle.writerablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author WangChenHol
 * @date 2021-10-31 10:11
 **/
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, Text> {
    // MapReduce默认是对KEY排序
    private final FlowSortBean outK = new FlowSortBean();
    private final Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        outV.set(split[1]);

        outK.setUpFlow(Long.valueOf(split[split.length - 3]));
        outK.setDownFlow(Long.valueOf(split[split.length - 2]));
        outK.setCountFlow();

        context.write(outK, outV);
    }
}
