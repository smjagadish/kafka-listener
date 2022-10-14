package com.example.listenerdemo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.stereotype.Component;

@SpringBootApplication
@ComponentScan("com.example.listenerconfig")
public class ListenerdemoApplication {

	public static void main(String[] args) {

		ApplicationContext ctx = SpringApplication.run(ListenerdemoApplication.class, args);
        messageconsumer obj = ctx.getBean(messageconsumer.class);
        obj.recieve();

	}

	@Component
	private class messageconsumer
	{
        @Autowired
		KafkaMessageListenerContainer<String,String> kmlc;
        // this will be an idle consumer as both the subscribed topics have single partition which is already allocated to the kmlc instance
		@Autowired
        ConcurrentMessageListenerContainer<String,String> cmlc;
        void recieve()
		{
		kmlc.start();
		cmlc.start();

		}
	}

}
