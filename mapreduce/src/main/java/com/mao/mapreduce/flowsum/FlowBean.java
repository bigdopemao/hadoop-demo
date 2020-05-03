package com.mao.mapreduce.flowsum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author bigdope
 * @create 2018-07-12
 **/
//public class FlowBean implements Writable, Comparable<FlowBean> {
public class FlowBean implements WritableComparable<FlowBean> {

    private String phone;

    private long upFlow;

    private long downFlow;

    private long sumFlow;

    //在反序列化时，反射机制需要调用空参构造函数，所以显示定义了一个空参构造函数
    public FlowBean() {
    }

    //为了对象数据的初始化方便，加入一个带参的构造函数
    public FlowBean(String phone, long upFlow, long downFlow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public void set(String phone, long upFlow, long downFlow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    //将对象数据序列化到流中
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phone);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    //从数据流中反序列出对象的数据
    //从数据流中读出对象字段时，必须跟序列化时的顺序保持一致
    public void readFields(DataInput dataInput) throws IOException {
        phone = dataInput.readUTF();
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
//        return "" +
//                ", upFlow=" + upFlow +
//                ", downFlow=" + downFlow +
//                ", sumFlow=" + sumFlow;

//        return "" + "\t" + upFlow + "\t" + downFlow + "\t" + sumFlow;
//        return "" + upFlow + "\t" + downFlow + "\t" + sumFlow;
        return phone + "\t" + upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public int compareTo(FlowBean o) {
        return sumFlow > o.getSumFlow() ? -1 : 1;
    }
}
