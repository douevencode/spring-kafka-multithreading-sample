package com.douevencode.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static com.douevencode.kafka.KafkaSpringNoAnnotationsApplication.CONSUMER_TOPIC;
import static com.douevencode.kafka.KafkaSpringNoAnnotationsApplication.PRODUCER_TOPIC;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class KafkaSpringNoAnnotationsApplicationTests {

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, false, 10, CONSUMER_TOPIC, PRODUCER_TOPIC);

    @Autowired
    MyKafkaConsumer consumer;

    @Autowired
    private TestRestTemplate restTemplate;

    /**
     * Assumption is that consumer is saving message from kafka with thread name appended.
     * Thus checking which stored messages are different shows how many threads were used
     * in consuming messages from Kafka
     */
    @Test
    public void consumerConsumesOnManyThreads() {
        int numberOfSentMessages = 10;
        IntStream.range(0, numberOfSentMessages).forEach(i -> sendMessageToKafka("Simple message", CONSUMER_TOPIC));

        await().atMost(1, SECONDS).until(() -> consumer.consumedMessages, hasSize(numberOfSentMessages));
        consumer.consumedMessages.forEach(System.out::println);
        long numOfDifferentThreads = consumer.consumedMessages.stream().distinct().count();
        assertThat(numOfDifferentThreads, greaterThanOrEqualTo(2L));
    }

    /**
     * Assumption is that controller is passing message to kafka with thread name appended.
     * Thus checking which received messages are different shows how many threads were used
     * in producing messages to Kafka
     */
    @Test
    public void producerProducesMessagesFromManyThreads() throws Exception {
        int numberOfSentRequests = 10;
        IntStream.range(0, numberOfSentRequests).forEach(i -> {
            restTemplate.postForEntity("/kafka/forward", "forwarded data", String.class);
        });

        ConsumerRecords<String, String> receivedRecords = getReceivedRecords(PRODUCER_TOPIC);
        assertThat(receivedRecords.count(), is(10));
        long numOfDifferentThreads = StreamSupport.stream(receivedRecords.spliterator(), false)
                .map(ConsumerRecord::value)
                .distinct()
                .count();

        assertThat(numOfDifferentThreads, greaterThanOrEqualTo(2L));
    }

    private ConsumerRecords<String, String> getReceivedRecords(String topic) throws Exception {
        Consumer<String, String> consumer =
                new DefaultKafkaConsumerFactory<String, String>(consumerProps())
                        .createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic);
        return KafkaTestUtils.getRecords(consumer, 3000);
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(UUID.randomUUID().toString(), "true", embeddedKafka);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private void sendMessageToKafka(String message, String topic) {
        Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka);
        Producer<Integer, String> producer = new DefaultKafkaProducerFactory<Integer, String>(props).createProducer();
        try {
            producer.send(new ProducerRecord<>(topic, message)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}