package com.mao.mapreduce.liuliang.enhance;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 读入原始日志数据，抽取其中的url，查询规则库，获得该url指向的网页内容的分析结果，追加到原始日志后
 * @author bigdope
 * @create 2018-09-07
 **/
// 读入原始数据 （47个字段） 时间戳 ..... destip srcip ... url .. . get 200 ...
// 抽取其中的url查询规则库得到众多的内容识别信息 网站类别，频道类别，主题词，关键词，影片名，主演，导演。。。。
// 将分析结果追加到原始日志后面
// context.write( 时间戳 ..... destip srcip ... url .. . get 200 ...
// 网站类别，频道类别，主题词，关键词，影片名，主演，导演。。。。)
// 如果某条url在规则库中查不到结果，则输出到带爬清单
// context.write( url tocrawl)
public class LogEnhanceMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static Map<String, String> ruleMap = new HashMap<String, String>();

    // setup方法是在mapper task 初始化时被调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //只运行一次，可以重载实现自己的功能，比如获得Configuration中的参数
        DBLoader.dbLoader(ruleMap);
//        dbLoader(ruleMap);
        ruleMap.put("111", "111");
        System.out.println("ruleMap size ========================= " + ruleMap.size());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        DBLoader.dbLoader(ruleMap);
        String line = value.toString();

        String[] fields = StringUtils.split(line, "\t");

        try {
//            if (fields.length > 27 && StringUtils.isNotEmpty(fields[26]) && fields[26].startsWith("http")) {
//                String url = fields[26];
//                String info = ruleMap.get(url);
//                String result = "";

            if (fields.length > 4 && StringUtils.isNotEmpty(fields[3]) && fields[3].startsWith("120.")) {
                String url = fields[3].trim();
                String info = ruleMap.get(url);
                String result = "";
//                if (info != null) {
//                    result = line + "\t" + info + "\n\r";
//                } else {
//                    result = url + "\t" + "toCrawl" + "\n\r";
//                }
                result = line + "\t" + info + " ruleMap size " + ruleMap.size() + "\n\r";
                context.write(new Text(result), NullWritable.get());
            } else {
                return;
            }
        } catch (Exception e) {
            System.out.println("exception occurred in mapper......");
        }

    }

}
