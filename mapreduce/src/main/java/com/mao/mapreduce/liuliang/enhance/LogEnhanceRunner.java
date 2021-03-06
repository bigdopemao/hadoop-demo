package com.mao.mapreduce.liuliang.enhance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * finalName; liuliang-enhance
 * hadoop jar liuliang-enhance.jar com.mao.mapreduce.liuliang.enhance.LogEnhanceRunner /liuliang/srclog /liuliang/output
 *
 * hadoop fs -cat /liuliang/output/enhanceLog
 * hadoop fs -cat /liuliang/output/enhanceLog
 * hadoop fs -cat /liuliang/output/toCrawl
 * @author bigdope
 * @create 2018-09-07
 **/
public class LogEnhanceRunner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conn = new Configuration();
        Job job = Job.getInstance(conn);

        job.setJarByClass(LogEnhanceRunner.class);

        job.setMapperClass(LogEnhanceMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(LogEnhanceOutputFormt.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LogEnhanceRunner(), args);
        System.exit(res);
    }

}
