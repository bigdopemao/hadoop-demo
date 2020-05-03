package com.mao.mapreduce.inverseIndex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author bigdope
 * @create 2018-07-29
 **/
//finalName  inverseindexcommonexecute
//hadoop jar inverseindexcommonexecute.jar com.mao.mapreduce.inverseIndex.InverseIndexCommonExecute /inverseindex/data /inverseindex/stepone1 /inverseindex/steptwo2
public class InverseIndexCommonExecute {

    public static class StepOneMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] fields = StringUtils.split(line, " ");

            FileSplit inputSplit = (FileSplit) context.getInputSplit();

            String fileName = inputSplit.getPath().getName();

            for (String field : fields) {
                context.write(new Text(field + "-->" + fileName), new LongWritable(1));
            }

        }
    }

    public static class StepOneReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long counter = 0;

            for (LongWritable value : values) {
                counter += value.get();
            }

            context.write(key, new LongWritable(counter));
        }
    }

    public static class StepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] fields = StringUtils.split(line, "\t");
            String[] wordAndFileName = StringUtils.split(fields[0], "-->");

            String word = wordAndFileName[0];
            String fileName = wordAndFileName[1];
            long count = Long.parseLong(fields[1]);

            context.write(new Text(word), new Text(fileName + "-->" + count));

        }
    }

    public static class StepTwoReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String result = "";

            for (Text value : values) {
                result += value + " ";
            }

            context.write(key, new Text(result));

        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //stepOneJob
        Job stepOneJob = Job.getInstance();

        stepOneJob.setJarByClass(InverseIndexCommonExecute.class);

        stepOneJob.setMapperClass(StepOneMapper.class);
        stepOneJob.setReducerClass(StepOneReducer.class);

        stepOneJob.setOutputKeyClass(Text.class);
        stepOneJob.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(stepOneJob, new Path(args[0]));

        //检查一下参数所指定的输出路径是否存在，如果已存在，先删除
        Path stepOneOutpath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(stepOneOutpath)) {
            fileSystem.delete(stepOneOutpath, true);
        }
        FileOutputFormat.setOutputPath(stepOneJob, stepOneOutpath);

        //stepTwoJob
        Job stepTwoJob = Job.getInstance();

        stepTwoJob.setJarByClass(InverseIndexCommonExecute.class);

        stepTwoJob.setMapperClass(StepTwoMapper.class);
        stepTwoJob.setReducerClass(StepTwoReducer.class);

        stepTwoJob.setOutputKeyClass(Text.class);
        stepTwoJob.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(stepTwoJob, stepOneOutpath);

        //检查一下参数所指定的输出路径是否存在，如果已存在，先删除
        Path stepTwoOutpath = new Path(args[2]);
        if (fileSystem.exists(stepTwoOutpath)) {
            fileSystem.delete(stepTwoOutpath, true);
        }
        FileOutputFormat.setOutputPath(stepTwoJob, stepTwoOutpath);

        //job execute
        //先提交stepOneJob，当stepOneJob执行完成且成功后执行stepTwoJob
        boolean stepOneJobResult = stepOneJob.waitForCompletion(true);
        if (stepOneJobResult) {
            boolean stepTwoJobResult = stepTwoJob.waitForCompletion(true);
            System.exit(stepTwoJobResult ? 0 : 1);
        }
        System.exit(stepOneJobResult ? 0 : 1);

    }

}
