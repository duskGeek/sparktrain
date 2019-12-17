package com.spark.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducer extends Thread{
  private String topic;

  Properties properties=new Properties();

  private Producer<Integer,String> producer;

  public KafkaProducer(String topic){
      this.topic=topic;

      properties.put("metadata.broker.list",KafkaProperties.BROKE_LIST);
      properties.put("serializer.class","kafka.serializer.StringEncoder");
      properties.put("request.required.acks","1");

      producer=new Producer<Integer, String>(new ProducerConfig(properties));
  }

    @Override
    public void run() {

      int messageNo=1;
      while (true){
        String message="message_"+messageNo;
        producer.send(new KeyedMessage<Integer, String>(topic,message));

        System.out.println("Sent :"+message);

        messageNo++;
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
      }
    }
}
