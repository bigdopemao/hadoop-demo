package com.mao.mapreduce.liuliang.topkurl;

import com.mao.mapreduce.flowsum.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author bigdope
 * @create 2018-09-05
 **/
public class TopkURLReducer extends Reducer<Text, FlowBean, Text, LongWritable> {

//    private Map<FlowBean, Text> treeMap = new TreeMap<>();
    private TreeMap<FlowBean, Text> treeMap = new TreeMap<FlowBean, Text>();
    private double globalCount = 0;

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Text url = new Text(key.toString());
        long upSum = 0;
        long downSum = 0;
        for (FlowBean flowBean : values) {
            upSum += flowBean.getUpFlow();
            downSum += flowBean.getDownFlow();
        }
        FlowBean bean = new FlowBean("", upSum, downSum);
        //每求得一条url的总流量，就累加到全局流量计数器中，等所有的
        // 记录处理完成后，globalCount中的值就是全局的流量总和
        globalCount += bean.getSumFlow();
        treeMap.put(bean, url);
    }

    //cleanup方法是在reduer任务即将退出时被调用一次
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<Map.Entry<FlowBean, Text>> entrySet = treeMap.entrySet();
        double tempCount = 0;
        for (Map.Entry<FlowBean, Text> entry : entrySet) {
            if (tempCount / globalCount < 0.8) {
                context.write(entry.getValue(), new LongWritable(entry.getKey().getSumFlow()));
                tempCount += entry.getKey().getSumFlow();
            } else {
                return;
            }
        }

    }
}
