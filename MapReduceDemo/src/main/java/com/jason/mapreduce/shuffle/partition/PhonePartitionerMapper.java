package com.jason.mapreduce.shuffle.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author WangChenHol
 * @date 2021-10-29 16:48
 **/
public class PhonePartitionerMapper extends Mapper<LongWritable, Text, Text, PhonePartitionerFlowBean> {
    private final Text outK = new Text();
    private final PhonePartitionerFlowBean outV = new PhonePartitionerFlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        String phone = split[1];
        String upFlow = split[split.length - 3];
        String downFlow = split[split.length - 2];

        outK.set(phone);

        outV.setUpFlow(Long.valueOf(upFlow));
        outV.setDownFlow(Long.valueOf(downFlow));
        outV.setCountFlow();

        context.write(outK, outV);

    }
}
