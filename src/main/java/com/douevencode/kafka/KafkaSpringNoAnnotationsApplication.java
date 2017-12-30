package com.douevencode.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@EnableAutoConfiguration
@SpringBootApplication
public class KafkaSpringNoAnnotationsApplication {

    static final String CONSUMER_TOPIC = "consumerTopic";
    static final String PRODUCER_TOPIC = "producerTopic";

    // default url of local Kafka instance
    static final String DEFAULT_KAFKA_URL = "http://localhost:9092";

    @Value("${spring.embedded.kafka.brokers}")
    private String brokerAddresses;

    public static void main(String[] args) {
        SpringApplication.run(KafkaSpringNoAnnotationsApplication.class, args);
    }

    @Bean
    public MyKafkaConsumer consumer() {
        MyKafkaConsumer consumer = new MyKafkaConsumer(brokerAddresses, CONSUMER_TOPIC);
        consumer.start();
        return consumer;
    }

    @Bean
    public KafkaForwardingController controller() {
        MyKafkaProducer producer = new MyKafkaProducer(brokerAddresses, "producerTopic");
        return new KafkaForwardingController(producer);
    }
}