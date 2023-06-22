package com.example.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class Asynchronous_Producer
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


            // Send the record to Kafka asynchronously
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("Message sent successfully. Topic: " +
                                recordMetadata.topic() + ", Partition: " +
                                recordMetadata.partition() + ", Offset: " +
                                recordMetadata.offset());
                    } else {
                        System.out.println("Message delivery failed: " + e.getMessage());
                    }
                }
            });
            Thread.sleep(1000);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }



    }

}
