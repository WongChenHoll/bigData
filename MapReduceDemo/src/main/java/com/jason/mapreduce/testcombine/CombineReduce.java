package com.jason.mapreduce.testcombine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 单词统计Reduce。
 * 泛型解释：
 * 1、Text：输入数据的Key
 * 2、IntWritable：输入数据的value
 * 3、Text：输出数据的key
 * 4、IntWritable：输出数据的value
 *
 * @author WangChenHol
 * @date 2021-10-22 16:04
 **/
public class CombineReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable v = new IntWritable();

    /**
     * 数据统计
     *
     * @param key     输出数据的key
     * @param values  输出数据的value
     * @param context 上下文环境
     * @throws IOException          异常
     * @throws InterruptedException 异常
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key, v);
    }
}
