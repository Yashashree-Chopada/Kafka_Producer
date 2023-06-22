package com.example.kafka.orderproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class OrderProducer
{
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        KafkaProducer<String, Integer> producer = new  KafkaProducer<>(props);
        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic1", "iPhone",10) ;

        try
        {
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println("Message Sent");
            System.out.println("Partition number = "+recordMetadata.partition());
            System.out.println("offset number = "+recordMetadata.offset());
            System.out.println("Topic name = "+recordMetadata.topic());


        } catch (Exception e) {
            e.printStackTrace();
        }
	
producer.flush();	
producer.close();


    }
}
