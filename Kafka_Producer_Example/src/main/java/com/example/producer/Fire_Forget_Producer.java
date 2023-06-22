package com.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.lang.ref.SoftReference;
import java.util.Properties;

public class Fire_Forget_Producer
{
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        KafkaProducer<String, Integer> producer = new  KafkaProducer<>(kafkaProps);
        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic1", "iPhone",10) ;
        try
        {
            producer.send(record);
           
            PrintRecord(record);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
producer.close();
    }

    private static void PrintRecord(ProducerRecord<String, Integer> record)
    {
        System.out.println("Message Sent");
        System.out.println("Partition number = "+record.partition());
        System.out.println("Key = "+record.key());
        System.out.println("Value = "+record.value());


    }
}
