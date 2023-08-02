package com.example.listenerconfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
public class ListenerController {

    @Autowired
    private KafkaListenerEndpointRegistry kreg;

    //Aug 02, 2023
    // Pausing and resuming the listener container(s) through spring schedules
    // The same code when added as a method in the bean wrapped in main class doesnt work
    // probably due to the fact the kreg bean isn't initialized yet ?
    @Scheduled(initialDelay = 20000, fixedDelay = 10000)
    void start_stop() throws InterruptedException {
        System.out.println("pausing");
        kreg.getListenerContainers().forEach(msg -> msg.pause());
        Thread.sleep(2000);
        System.out.println("resuming");
        kreg.getListenerContainers().forEach(msg -> msg.resume());
    }
}
