package com.jason.mapreduce.shuffle.sortpartition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author WangChenHol
 * @date 2021-10-31 10:11
 **/
public class FlowPartitionSortMapper extends Mapper<LongWritable, Text, FlowPartitionSortBean, Text> {
    // MapReduce默认是对KEY排序
    private final FlowPartitionSortBean outK = new FlowPartitionSortBean();
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
