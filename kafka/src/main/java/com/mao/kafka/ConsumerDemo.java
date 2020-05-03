package com.mao.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author bigdope
 * @create 2018-08-31
 **/
public class ConsumerDemo {

    private static String topic = "mygirls";
    private final static Integer threads = 1;

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "192.168.2.2:2181");
        properties.put("group.id", "1111");
        properties.put("auto.offset.reset", "smallest");

        ConsumerConfig config = new ConsumerConfig(properties);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, threads);
        topicCountMap.put("myboys", 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> mygirlsStreams = messageStreams.get("mygirls");

        for (final KafkaStream<byte[], byte[]> mygirlsStream : mygirlsStreams) {

//            new Thread(new Runnable() {
//                public void run() {
//                    for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : mygirlsStream) {
//                        String message = new String(messageAndMetadata.message());
//                        System.out.println(message);
//                    }
//                }
//            }).start();

            for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : mygirlsStream) {
                String message = new String(messageAndMetadata.message());
                System.out.println(message);
            }

        }


    }

}
