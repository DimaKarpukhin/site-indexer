package com.handson.siteIndexer.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.handson.siteIndexer.configs.KafkaEmbeddedConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.*;


public class Kafka {
    public Kafka(KafkaConsumer<String, String> consumer, KafkaProducer<String, String> producer, ObjectMapper om) {
        this.consumer = consumer;
        this.producer = producer;
        this.om = om;
    }

    KafkaConsumer<String,String> consumer;
    KafkaProducer<String,String> producer;
    ObjectMapper om;


    public  <T> List<T> receive(Class<T> targetClazz)  {
        List<T> res = new ArrayList<>();
        consumer.poll(Duration.ofSeconds(1)).forEach(x-> {
            try {
                res.add(
                        om.readValue(x.value(), targetClazz)
                );
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return res;
    }

    public  <T> boolean send(T record)  {
        try {
            String message = om.writeValueAsString(record);
            producer.send(new ProducerRecord<>(
                    KafkaEmbeddedConfig.TEST_TOPIC,
                    UUID.randomUUID().toString(),
                    message))
                    .get();
            producer.flush();
            return true;
        }catch (Exception e) {
            return false;
        }
    }
}
