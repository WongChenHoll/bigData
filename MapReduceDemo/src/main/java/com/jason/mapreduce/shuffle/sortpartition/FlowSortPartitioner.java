package com.jason.mapreduce.shuffle.sortpartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区规则
 *
 * @author WangChenHol
 * @date 2021-10-29 16:34
 **/
public class FlowSortPartitioner extends Partitioner<FlowPartitionSortBean, Text> {
    /**
     * 分区
     *
     * @param text          对应文件中的手机号
     * @param bean          手机流量对象
     * @param numPartitions 分区的总数量
     * @return 分区号
     */
    @Override
    public int getPartition(FlowPartitionSortBean bean, Text text, int numPartitions) {

        //根据手机的前三位进行分区
        String par = text.toString().substring(0, 3);
        switch (par) {
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
