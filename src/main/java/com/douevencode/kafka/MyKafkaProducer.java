package com.douevencode.kafka;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class MyKafkaProducer {

    private final String topic;
    private KafkaTemplate<String, String> kafkaTemplate;

    MyKafkaProducer(String brokerAddress, String topic) {
        this.topic = topic;
        kafkaTemplate = createTemplate(brokerAddress);
    }

    public ListenableFuture<SendResult<String, String>> send(String message) {
        return kafkaTemplate.send(topic, message);
    }

    private Map<String, Object> producerConfig(String brokerAddress) {
        return Map.of(
                BOOTSTRAP_SERVERS_CONFIG, brokerAddress
        );
    }

    private KafkaTemplate<String, String> createTemplate(String brokerAddress) {
        return new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(
                        producerConfig(brokerAddress),
                        new StringSerializer(),
                        new StringSerializer()));
    }
}
