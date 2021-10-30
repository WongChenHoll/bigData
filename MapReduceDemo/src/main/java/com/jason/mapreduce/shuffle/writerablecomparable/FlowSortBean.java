package com.jason.mapreduce.shuffle.writerablecomparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author WangChenHol
 * @date 2021-10-30 11:57
 **/
public class FlowSortBean implements WritableComparable<FlowSortBean> {
    private Long upFlow; //上行流量
    private Long downFlow; // 下行流量
    private Long countFlow; // 总流量

    @Override
    public int compareTo(FlowSortBean o) {
        // 总流量倒序
        if (this.countFlow > o.countFlow) {
            return -1;
        } else if (this.countFlow < o.countFlow) {
            return 1;
        } else {
            // 上行流量正序
            return this.upFlow.compareTo(o.upFlow);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.countFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.countFlow = in.readLong();
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
