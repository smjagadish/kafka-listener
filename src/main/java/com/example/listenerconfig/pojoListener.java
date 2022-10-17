package com.example.listenerconfig;

import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component

public class pojoListener {
    @KafkaListener(topics = "sample-topic4",autoStartup = "${listen.auto.start:true}",groupId = "overridden-listener")
    public void listen(String data , Acknowledgment ack)
    {
        System.out.println(data);
        ack.acknowledge();
    }
}
