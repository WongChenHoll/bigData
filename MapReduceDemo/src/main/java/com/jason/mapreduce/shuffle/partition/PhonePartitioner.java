package com.jason.mapreduce.shuffle.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author WangChenHol
 * @date 2021-10-29 16:34
 **/
public class PhonePartitioner extends Partitioner<Text, PhonePartitionerFlowBean> {
    /**
     * 分区
     *
     * @param text               对应文件中的手机号
     * @param partitionPhoneFlow 手机流量对象
     * @param numPartitions      分区的总数量
     * @return 分区号
     */
    @Override
    public int getPartition(Text text, PhonePartitionerFlowBean partitionPhoneFlow, int numPartitions) {
        String phone = text.toString();

        //根据手机的前三位进行分区
        String prePhone = phone.substring(0, 3);

        switch (prePhone) {
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
