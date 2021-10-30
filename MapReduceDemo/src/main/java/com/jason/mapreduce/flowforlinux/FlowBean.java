package com.jason.mapreduce.flowforlinux;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 统计手机的上行、下行、总的流量。
 * 该类为了序列化必须实现org.apache.hadoop.io.Writable接口。
 *
 * @author WangChenHol
 * @date 2021-10-25 17:17
 **/
public class FlowBean implements Writable {
    /**
     * 反序列化时，需要反射调用空参构造函数，所以必须有空参构造
     */
    public FlowBean() {
    }

    private Long upFlow;
    private Long downFlow;
    private Long countFlow;

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
}
