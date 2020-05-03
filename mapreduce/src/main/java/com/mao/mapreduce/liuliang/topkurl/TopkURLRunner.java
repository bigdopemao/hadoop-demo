package com.mao.mapreduce.liuliang.topkurl;

import com.mao.mapreduce.flowsum.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * finalName: liuliang
 * hadoop jar liuliang.jar com.mao.mapreduce.liuliang.topkurl.TopkURLRunner /liuliang/srclog /liuliang/topkurl
 * @author bigdope
 * @create 2018-09-05
 **/
public class TopkURLRunner extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance();

        job.setJarByClass(TopkURLRunner.class);

        job.setMapperClass(TopkURLMapper.class);
        job.setReducerClass(TopkURLReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopkURLRunner(), args);
        System.exit(res);
    }

}
