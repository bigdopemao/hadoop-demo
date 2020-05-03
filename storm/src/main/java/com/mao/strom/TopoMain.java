package com.mao.strom;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * finalname: demotopo
 * sh storm jar /home/hadoop/jar/stormJar/demotopo.jar com.mao.strom.TopoMain
 * 组织各个处理逐渐形成一个完整的处理流程，就是所谓的topology(类似mapreduce程序中的job)
 * 并且将该topology提交给storm集群去运行，topology提交到集群后就将无休止的运行，除非人为或者异常退出
 * @author bigdope
 * @create 2018-08-30
 **/
public class TopoMain {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        //将我们spout组件设置到topology中去
        //parallelism_hint : 4小时用4个executor来执行这个组件
        //setNumTasks(8) 设置的是该组件执行时的并发task数量，也就意味着1个executor会执行2个task
        builder.setSpout("randomspout", new RandomWordSpout(), 4);

        //将大写转换bolt组件设置到topology，并且指定它接收randomspout组件的消息
        //.shuffleGrouping("randomspout")包含两层含义：
        //1、upperbolt组件接收的tuple消息一定来自于randomspout组件
        //2、randomspout组件和upperbolt组件的大量并发task实例之间收发消息时采用的分组策略是随机分组shuffleGrouping
        builder.setBolt("upperbolt", new UpperBolt(), 4).shuffleGrouping("randomspout");

        //将添加后缀的bolt组件设置到topology，并且指定它接收upperbolt组件的消息
        builder.setBolt("suffixbolt", new SuffixBolt(), 4).shuffleGrouping("upperbolt");

        //用builder来创建一个topology
        StormTopology topology = builder.createTopology();

        //配置一些topology在急群众运行时的参数
        Config config = new Config();
        //这里设置的是整个topology所占的槽位数，也就是worker的数量
        config.setNumWorkers(4);
        config.setDebug(true);
        config.setNumAckers(0);

        // 将这个topology提交给storm集群运行
        StormSubmitter.submitTopology("demotopo", config, topology);

    }}
