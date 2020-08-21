package com.handson.siteIndexer.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.handson.siteIndexer.config.KafkaEmbeddedConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class KafkaHelper {
    public KafkaHelper(KafkaConsumer<String, String> consumer, KafkaProducer<String, String> producer, ObjectMapper om) {
        this.consumer = consumer;
        this.producer = producer;
        this.om = om;
    }

    KafkaConsumer<String,String> consumer;
    KafkaProducer<String,String> producer;
    ObjectMapper om;


    public  <T> List<T> recieve( Class<T> targetClazz)  {
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
            producer.send(new ProducerRecord<String,String>(KafkaEmbeddedConfig.TEST_TOPIC, UUID.randomUUID().toString(),message)).get();
            producer.flush();
            return true;
        }catch (Exception e) {
            return false;
        }
    }
}
