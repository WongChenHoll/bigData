package com.jason.mapreduce.shuffle.reduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author WangChenHol
 * @date 2021-11-1 16:42
 **/
public class TableBean implements Writable {

    private String orderId; // 订单ID
    private String productId; //产品ID
    private int amount; //数量
    private String productName; // 产品名字
    private String sourceFlag; // 来源哪个表标志，order：订单表，product：产品表

    // 序列化必须有的空参构造函数
    public TableBean() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.orderId);
        out.writeUTF(this.getProductId());
        out.writeInt(this.amount);
        out.writeUTF(this.productName);
        out.writeUTF(sourceFlag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.productId = in.readUTF();
        this.amount = in.readInt();
        this.productName = in.readUTF();
        this.sourceFlag = in.readUTF();
    }

    @Override
    public String toString() {
        return orderId + "\t" + productName + "\t" + amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getSourceFlag() {
        return sourceFlag;
    }

    public void setSourceFlag(String sourceFlag) {
        this.sourceFlag = sourceFlag;
    }
}
