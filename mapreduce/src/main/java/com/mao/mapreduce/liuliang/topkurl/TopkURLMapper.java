package com.mao.mapreduce.liuliang.topkurl;

import com.mao.mapreduce.flowsum.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author bigdope
 * @create 2018-09-05
 **/
public class TopkURLMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private FlowBean bean = new FlowBean();
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = StringUtils.split(line, "\t");

        try {
//            if (fields.length > 32 && StringUtils.isNotEmpty(fields[26]) && fields[26].startsWith("http")) {
//                String url = fields[26];
//                long upFlow = Long.parseLong(fields[30]);
//                long downFlow = Long.parseLong(fields[31]);
            if (fields.length > 8 && StringUtils.isNotEmpty(fields[3]) && fields[3].startsWith("120.")) {
                String url = fields[3];
                long upFlow = Long.parseLong(fields[7]);
                long downFlow = Long.parseLong(fields[8]);

                text.set(url);
                bean.set("", upFlow, downFlow);

                context.write(text, bean);
            }
        } catch (Exception e) {
            System.out.println("exception occurred in mapper.....");
        }

    }
}
