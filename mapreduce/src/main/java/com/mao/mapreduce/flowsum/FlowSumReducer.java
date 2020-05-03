package com.mao.mapreduce.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author bigdope
 * @create 2018-07-12
 **/
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    //框架每传递一组数据<1387788654,{flowbean,flowbean,flowbean,flowbean.....}>调用一次我们的reduce方法
    //reduce中的业务逻辑就是遍历values，然后进行累加求和再输出
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long upFlowCount = 0;
        long downFlowCount = 0;

        for (FlowBean value : values) {
            upFlowCount += value.getUpFlow();
            downFlowCount += value.getDownFlow();
        }

        context.write(key, new FlowBean(key.toString(), upFlowCount, downFlowCount));
    }
}
