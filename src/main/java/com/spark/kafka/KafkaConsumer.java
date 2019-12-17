package com.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer extends Thread{

    private String topic;

    public KafkaConsumer(String topic){
        this.topic=topic;

    }


    private ConsumerConnector createConn(){
        Properties properties=new Properties();
        properties.put("zookeeper.connect",KafkaProperties.ZK);
        properties.put("group.id",KafkaProperties.GROUP_ID);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {
        ConsumerConnector consumer=createConn();

        Map<String,Integer> topicConfig=new HashMap<String, Integer>();
        topicConfig.put(topic,1);

        //String topic
        // List<KafkaStream<byte[], byte[]>> 对应的数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStream=consumer.createMessageStreams(topicConfig);

        KafkaStream<byte[], byte[]> stream= messageStream.get(topic).get(0);

        ConsumerIterator<byte[],byte[]> iterator=stream.iterator();

        while (iterator.hasNext()){
            String message=new String(iterator.next().message());

            System.out.println("rec:"+message);
        }
    }
}
