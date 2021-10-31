package com.jason.mapreduce.shuffle.writerablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author WangChenHol
 * @date 2021-10-31 10:16
 **/
public class FlowSortReducer extends Reducer<FlowSortBean, Text, Text, FlowSortBean> {
    @Override
    protected void reduce(FlowSortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
