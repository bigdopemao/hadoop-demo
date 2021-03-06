package com.mao.strom;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * @author bigdope
 * @create 2018-08-30
 **/
public class SuffixBolt extends BaseBasicBolt {

    FileWriter fileWriter = null;

    //在bolt组件运行过程中只会被调用一次
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        try {
            fileWriter = new FileWriter("/home/hadoop/stormoutput/" + UUID.randomUUID());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //该bolt组件的核心处理逻辑
    //没收到一个tuple消息，就会被调用一次
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        //先拿到上一个组件发送过来的商品名称
        String upperName = tuple.getString(0);
        String suffixName = upperName + " 2018-8-30";

        //为上一个组件发送过来的商品名称添加后缀

        try {
            fileWriter.write(suffixName);
            fileWriter.write("\n");
            fileWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //本bolt已经不需要发送tuple消息到下一个组件，所以不需要再声明tuple的字段
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

}
