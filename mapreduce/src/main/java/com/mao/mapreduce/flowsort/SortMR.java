package com.mao.mapreduce.flowsort;

import com.mao.mapreduce.flowsum.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author bigdope
 * @create 2018-07-12
 **/
public class SortMR {

    public static class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {

        //拿到一行数据，切分出各字段，封装为一个flowbean，作为key输出
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = StringUtils.split(line, "\t");

            String phone = fields[0];
            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);

            context.write(new FlowBean(phone, upFlow, downFlow), NullWritable.get());
        }
    }

    public static class SortReducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {

        @Override
        protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(key, NullWritable.get());

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job flowSumJob = Job.getInstance(conf);

        flowSumJob.setJarByClass(SortMR.class);

        flowSumJob.setMapperClass(SortMapper.class);
        flowSumJob.setReducerClass(SortReducer.class);

//        flowSumJob.setMapOutputKeyClass(FlowBean.class);
//        flowSumJob.setMapOutputValueClass(NullWritable.class);

        flowSumJob.setOutputKeyClass(FlowBean.class);
        flowSumJob.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(flowSumJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(flowSumJob, new Path(args[1]));

        System.exit(flowSumJob.waitForCompletion(true) ? 0 : 1);

    }

}
