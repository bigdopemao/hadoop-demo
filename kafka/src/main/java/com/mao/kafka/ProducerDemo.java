package com.mao.kafka;

import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author bigdope
 * @create 2018-08-31
 **/
public class ProducerDemo {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("zk.connect", "192.168.2.2:2181");
        properties.put("metadata.broker.list", "192.168.2.2:9092,192.168.2.2:9093,192.168.2.2:9094");
//        properties.put("metadata.broker.list", "192.168.2.2:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");

//        properties.put("bootstrap.servers", "192.168.2.2:9092");
        properties.put("bootstrap.servers", "192.168.2.2:9092,192.168.2.2:9093,192.168.2.2:9094");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("key.serializer", StringEncoder.class.getName());
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        properties.put("advertised.listeners", "PLAINTEXT://192.168.2.2:9092");

        ProducerConfig config = new ProducerConfig(properties);
//        Producer<String, String> producer = new Producer<String, String>(config);

        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        //发送业务消息
        //读取文件 读取内存数据库 读取socket端口
        for (int i = 0; i < 100; i++) {
            Thread.sleep(500);
//            producer.send((Seq<KeyedMessage<String, String>>) new KeyedMessage<String, String>("mygirls",
//                    "i say love you baby for " + i + " times"));

            producer.send(new ProducerRecord<String, String>("mygirls",
                    "i say love you baby for " + i + " times"));
        }

    }

}
