package com.douevencode.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class MyKafkaConsumer {

    private final String brokerAddress;
    private final String topic;

    List<String> consumedMessages = new ArrayList<>();

    MyKafkaConsumer(String brokerAddress, String topic) {
        this.brokerAddress = brokerAddress;
        this.topic = topic;
    }

    void start() {
        MessageListener<String, String> messageListener = record -> {
            System.out.println("I received message on thread: " + Thread.currentThread().getName());
            consumedMessages.add(record.value() + Thread.currentThread().getName());
        };

        ConcurrentMessageListenerContainer container =
                new ConcurrentMessageListenerContainer<>(
                        consumerFactory(brokerAddress),
                        containerProperties(topic, messageListener));

        container.setBeanName("NAME");
        container.setConcurrency(10);
        container.start();
    }

    private DefaultKafkaConsumerFactory<String, String> consumerFactory(String brokerAddress) {
        return new DefaultKafkaConsumerFactory<>(
                consumerConfig(brokerAddress),
                new StringDeserializer(),
                new StringDeserializer());
    }

    private ContainerProperties containerProperties(String topic, MessageListener<String, String> messageListener) {
        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setMessageListener(messageListener);
        return containerProperties;
    }

    private Map<String, Object> consumerConfig(String brokerAddress) {
        return Map.of(
                    BOOTSTRAP_SERVERS_CONFIG, brokerAddress,
                    GROUP_ID_CONFIG, "groupId",
                    AUTO_OFFSET_RESET_CONFIG, "earliest"
            );
    }
}
