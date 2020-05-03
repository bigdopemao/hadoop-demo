package com.mao.strom;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author bigdope
 * @create 2018-08-30
 **/
public class UpperBolt extends BaseBasicBolt {

    //业务处理逻辑
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //先获取到上一个组件传递过来的数据，数据在tuple里面
        String goodName = tuple.getString(0);

        //将商品名称转化成大写
        String goodNameUpper = goodName.toUpperCase();

        //将转化完成的商品名发送出去
        basicOutputCollector.emit(new Values(goodNameUpper));

    }

    //声明该bolt组件要发出去的tuple的字段
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("upperName"));
    }

}
