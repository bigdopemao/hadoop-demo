package com.mao.mapreduce.areapartition;

import com.mao.mapreduce.flowsum.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 对流量原始日志进行流量统计，将不同省份的用户统计结果输出到不同文件
 * 需要自定义改造两个机制：
 * 1、改造分区的逻辑，自定义一个partitioner
 * 2、自定义reduer task的并发任务数
 * @author bigdope
 * @create 2018-07-12
 **/
//finalName  flowsumarea
public class FlowSumArea {

    public static class FlowSumAreaMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //拿一行数据
            String line = value.toString();
            //切分成各个字段
            String[] fields = StringUtils.split(line, "\t");

            //拿到我们需要的字段
            String phone = fields[1];
            long upFlow = Long.parseLong(fields[7]);
            long downFlow = Long.parseLong(fields[8]);

            //封装数据为kv并输出
            context.write(new Text(phone), new FlowBean(phone, upFlow, downFlow));

        }
    }

    public static class FlowSumAreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

            long upFlow = 0;
            long downFlow = 0;

            for (FlowBean value : values) {
                upFlow += value.getUpFlow();
                downFlow += value.getDownFlow();
            }

            context.write(key, new FlowBean(key.toString() , upFlow, downFlow));

        }
    }

    public static void main(String[] args) throws Exception {

        Job flowSumAreaJob = Job.getInstance();

        flowSumAreaJob.setJarByClass(FlowSumArea.class);

        flowSumAreaJob.setMapperClass(FlowSumAreaMapper.class);
        flowSumAreaJob.setReducerClass(FlowSumAreaReducer.class);

        //设置我们自定义的分组逻辑定义
        flowSumAreaJob.setPartitionerClass(AreaPartition.class);

        flowSumAreaJob.setOutputKeyClass(Text.class);
        flowSumAreaJob.setOutputValueClass(FlowBean.class);

        //设置reduce的任务并发数，应该跟分组的数量保持一致
        flowSumAreaJob.setNumReduceTasks(6);

        FileInputFormat.setInputPaths(flowSumAreaJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(flowSumAreaJob, new Path(args[1]));

        System.exit(flowSumAreaJob.waitForCompletion(true) ? 0 : 1);

    }

}
