package com.mao.strom;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * @author bigdope
 * @create 2018-08-29
 **/
public class RandomWordSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;

    String[] words = {"iphone", "xiaomi", "mate", "sony", "sumsung", "moto", "meizu"};

    //初始化方法，在spout组件实例化时调用一次
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    //不断的往下一个组件发送tuple消息
    //这里面是该spout组件的核心逻辑
   public void nextTuple() {
       Random random = new Random();
       int index = random.nextInt(words.length);
       //通过随机数拿到一个商品名
       String goodName = words[index];

       //将商品名分装为tuple，发送消息给下一个组件
       spoutOutputCollector.emit(new Values(goodName));

       //每发送一个消息，休眠500ms
       Utils.sleep(500);

   }

   //声明本spout组件发送出去的tuple中的数据的字段名
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("originName"));

    }
}
