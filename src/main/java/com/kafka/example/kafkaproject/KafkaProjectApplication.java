package com.kafka.example.kafkaproject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@SpringBootApplication
public class KafkaProjectApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaProjectApplication.class, args);
        kafkaPublisher();
        kafkaConsumer();
    }

    private static void kafkaPublisher() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // SSL Properties
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.location", "<path>/server.truststore.jks");
        properties.put("ssl.truststore.password", "kafka123");
        properties.put("ssl.keystore.location", "<path>/server.keystore.jks");
        properties.put("ssl.keystore.password", "kafka123");
        properties.put("ssl.key.password", "kafka123");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        try {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<String, String>("Topic1", "Good Job : " + i));
                System.out.println("Produced message : " + " Good Job : " + i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }

    private static void kafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "group1");
        //SSL properties
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.location", "<path>/server.truststore.jks");
        properties.put("ssl.truststore.password", "kafka123");
        properties.put("ssl.keystore.location", "<path>/server.keystore.jks");
        properties.put("ssl.keystore.password", "kafka123");
        properties.put("ssl.key.password", "kafka123");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        ;
        try {
            List<String> topics = new ArrayList<String>();
            topics.add("Topic1");
            kafkaConsumer.subscribe(topics);
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            kafkaConsumer.commitAsync();
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println("Consumed message : " + record.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }

}
