package com.spark.kafka;

public class KafkaClientApp {

    public static void main(String[] args){
        KafkaProducer kafkaProducer=new KafkaProducer(KafkaProperties.TOPIC);
        kafkaProducer.start();

        KafkaConsumer kafkaConsumer=new KafkaConsumer(KafkaProperties.TOPIC);

        kafkaConsumer.start();
    }
}
