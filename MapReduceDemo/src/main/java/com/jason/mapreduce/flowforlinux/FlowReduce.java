package com.jason.mapreduce.flowforlinux;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author WangChenHol
 * @date 2021-10-26 16:33
 **/
public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
    private final FlowBean flow = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Long up = 0L;
        Long down = 0L;

        for (FlowBean next : values) {
            up += next.getUpFlow();
            down += next.getDownFlow();
        }
        flow.setUpFlow(up);
        flow.setDownFlow(down);
        flow.setCountFlow();
        context.write(key, flow);
    }
}
