package com.jason.mapreduce.shuffle.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 对手机流量分区统计
 *
 * @author WangChenHol
 * @date 2021-10-29 16:38
 **/
public class PhonePartitionerFlowBean implements Writable {

    private Long upFlow; //上行流量
    private Long downFlow; // 下行流量
    private Long countFlow; // 总流量

    public PhonePartitionerFlowBean() {
    }

    /**
     * 必须重写序列化方法
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.upFlow);
        dataOutput.writeLong(this.downFlow);
        dataOutput.writeLong(this.countFlow);
    }

    /**
     * 必须重写反序列化方法
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.countFlow = dataInput.readLong();
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getCountFlow() {
        return countFlow;
    }

    public void setCountFlow(Long countFlow) {
        this.countFlow = countFlow;
    }

    public void setCountFlow() {
        this.countFlow = this.upFlow + this.downFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + countFlow;
    }
}
