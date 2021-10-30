package com.jason.mapreduce.shuffle.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author WangChenHol
 * @date 2021-10-29 16:55
 **/
public class PhonePartitionerReduce extends Reducer<Text, PhonePartitionerFlowBean, Text, PhonePartitionerFlowBean> {
    private final PhonePartitionerFlowBean flow = new PhonePartitionerFlowBean();

    @Override
    protected void reduce(Text key, Iterable<PhonePartitionerFlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlowSum = 0;
        long downFlowSum = 0;

        for (PhonePartitionerFlowBean value : values) {
            upFlowSum += value.getUpFlow();
            downFlowSum += value.getDownFlow();
        }
        flow.setUpFlow(upFlowSum);
        flow.setDownFlow(downFlowSum);
        flow.setCountFlow();

        context.write(key, flow);
    }
}
