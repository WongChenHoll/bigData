package com.jason.mapreduce.shuffle.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * @author WangChenHol
 * @date 2021-11-1 17:42
 **/
public class TableMapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    // 将产品的ID和名字存放在内存的Map中
    private HashMap<String, String> productMap = new HashMap<>();
    private Text outK = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 通过缓存得到小文件path
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        // 获取流
        FileSystem fileSystem = FileSystem.get(new Configuration());
        FSDataInputStream inputStream = fileSystem.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

        // 按行读取处理数据
        String line;
        while (StringUtils.isNoneBlank(line = reader.readLine())) {
            String[] split = line.split("\t");
            productMap.put(split[0], split[1]);
        }
        IOUtils.closeStream(reader);
        IOUtils.closeStream(inputStream);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        outK.set(split[0] + "\t" + productMap.get(split[1]) + "\t" + split[2]);
        context.write(outK, NullWritable.get());
    }
}
