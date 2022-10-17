package com.example.listenerdemo;

import com.example.listenerconfig.pojoListener;
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
		pojoListener obj2 = ctx.getBean(pojoListener.class);

		// obj.recieve();
        System.out.println("program term");
	}

	@Component
	private class messageconsumer
	{
       @Autowired
		KafkaMessageListenerContainer<String,String> kmlc;
        // this will be an idle consumer as both the subscribed topics have single partition which is already allocated to the kmlc instance via the range partition strategy
		// changing partition strategy to round robin will mean that kmlc and cmlc will each get a topic (and its respective partition) due to the shared consumer property object
		@Autowired
        ConcurrentMessageListenerContainer<String,String> cmlc;
        void recieve()
		{
			// looks like these are optional ???
			// auto-startup means these are redundant code 
		kmlc.start();
		cmlc.start();

		}
	}

}
