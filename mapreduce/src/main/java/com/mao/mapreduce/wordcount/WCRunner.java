package com.mao.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 用来描述一个特定的作业
 * 比如，该作业使用哪个类作为逻辑处理中的map，哪个作为reduce
 * 还可以指定该作业要处理的数据所在的路径
 * 还可以指定改作业输出的结果放到哪个路径
 * @author bigdope
 * @create 2018-07-09
 **/
public class WCRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

//        Configuration conf = new Configuration();
//        Job wcJob = Job.getInstance(conf);

        Job wcJob = Job.getInstance();

        //设置整个job所用的那些类在哪个jar包
        wcJob.setJarByClass(WCRunner.class);

        //本job使用的mapper和reducer的类
        wcJob.setMapperClass(WCMapper.class);
        wcJob.setReducerClass(WCReducer.class);

        //指定reduce的输出数据kv类型
        wcJob.setMapOutputKeyClass(Text.class);
        wcJob.setMapOutputValueClass(LongWritable.class);

        //指定mapper的输出数据kv类型
        wcJob.setOutputKeyClass(Text.class);
        wcJob.setMapOutputValueClass(LongWritable.class);

        //指定要处理的输入数据存放路径
        FileInputFormat.setInputPaths(wcJob, new Path("/wc/srcdata/"));
        //指定处理结果的输出数据存放路径
        FileOutputFormat.setOutputPath(wcJob, new Path("/wc/output/"));

        //将job提交给集群运行
        wcJob.waitForCompletion(true);

    }

}
