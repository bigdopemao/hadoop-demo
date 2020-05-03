package com.mao.storm_kafka;

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
 * @create 2018-09-03
 **/
public class WriterBolt extends BaseBasicBolt {

    private FileWriter writer = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            writer = new FileWriter("d:\\storm_kafka\\" + "wordcount" + UUID.randomUUID().toString());
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String s = tuple.getString(0);
        try {
            writer.write(s);
            writer.write("\n");
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
