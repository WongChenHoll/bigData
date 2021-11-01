package com.jason.mapreduce.shuffle.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author WangChenHol
 * @date 2021-11-1 16:50
 **/
public class TableReduceJoinMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String fileName;
    private final Text outK = new Text();
    private final TableBean outV = new TableBean();

    // MapTask开始时只调用一次的方法。Called once at the beginning of the task.
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取分片信息
        FileSplit split = (FileSplit) context.getInputSplit();
        // 获取文件名称
        fileName = split.getPath().getName();
    }

    // 产品ID作为输出 KEY
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");


        if (fileName.contains("order")) {
            outK.set(fields[1]);

            outV.setOrderId(fields[0]);
            outV.setProductId(fields[1]);
            outV.setAmount(Integer.parseInt(fields[2]));
            outV.setProductName("");
            outV.setSourceFlag("order");
        } else {
            outK.set(fields[0]);

            outV.setOrderId("");
            outV.setAmount(0);
            outV.setProductId(fields[0]);
            outV.setProductName(fields[1]);
            outV.setSourceFlag("product");
        }
        context.write(outK, outV);
    }
}
