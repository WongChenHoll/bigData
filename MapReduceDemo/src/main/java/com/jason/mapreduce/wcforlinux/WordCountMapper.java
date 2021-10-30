package com.jason.mapreduce.wcforlinux;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 单词统计Mapper。
 * 泛型解释：
 * 1、LongWritable：输入key，文件中数据的偏移量
 * 2、Text：输入value：文件中的数据
 * 3、Text：输出key：文件中的数据
 * 4、IntWritable：输出value：单词的个数
 *
 * @author WangChenHol
 * @date 2021-10-22 15:55
 **/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text k = new Text();
    private final IntWritable v = new IntWritable(1);

    /**
     * 每一行数据处理
     *
     * @param key     偏移量
     * @param value   文件中的单词数据
     * @param context 上下文环境
     * @throws IOException          异常
     * @throws InterruptedException 异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
