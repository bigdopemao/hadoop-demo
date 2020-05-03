package com.mao.storm_kafka;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

/**
 * @author bigdope
 * @create 2018-09-03
 **/
public class WordSpliterBolt extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String line = tuple.getString(0);
        String[] words = line.split(" ");
        for (String word : words) {
            word = word.trim();
            if (StringUtils.isNotBlank(word)) {
                word = word.toLowerCase();
                basicOutputCollector.emit(new Values(word));
            }
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

}
