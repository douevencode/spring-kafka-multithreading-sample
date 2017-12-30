package com.douevencode.kafka;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import static org.springframework.http.HttpStatus.OK;

@RequestMapping(path = "/kafka")
public class KafkaForwardingController {

    private final MyKafkaProducer kafkaProducer;

    KafkaForwardingController(MyKafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @RequestMapping(path = "/forward")
    public ResponseEntity<?> forwardToProducer(@RequestBody String messageToForward) {
        System.out.println("Sending message from thread " + Thread.currentThread().getName());
        kafkaProducer.send(messageToForward + " " + Thread.currentThread().getName());
        return new ResponseEntity<>(OK);
    }
}
