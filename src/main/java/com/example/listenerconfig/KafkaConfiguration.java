package com.example.listenerconfig;

import com.example.mapinterface.objinf;
import com.example.wrapper.userInfo;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.ParseStringDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Configuration
@EnableKafka
public class KafkaConfiguration {
    @Autowired
    KafkaProperties properties;

    @Bean
    public Map<String,Object> config_src()
    {
        Map<String,Object> config = new HashMap<>();
        config.put(BOOTSTRAP_SERVERS_CONFIG,"localhost:29092");
        config.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        config.put(GROUP_ID_CONFIG,"process-1");
        config.put(ENABLE_AUTO_COMMIT_CONFIG,false);
      //  config.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        return config;
    }
    @Bean
    public Map<String,Object> config_src1()
    {
        Map<String,Object> config = new HashMap<>();
        config.put(BOOTSTRAP_SERVERS_CONFIG,"localhost:29092");
        config.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(VALUE_DESERIALIZER_CLASS_CONFIG,JsonDeserializer.class);
        config.put(GROUP_ID_CONFIG,"process-1");
        config.put(ENABLE_AUTO_COMMIT_CONFIG,false);
        config.put(JsonDeserializer.TRUSTED_PACKAGES,"*");
        // type info headers packed by serializer are handled here
        // this will supersede the class type in deserializer instance in the consumer factory if usetypeinfoheaders arg is not passed as false
        config.put(JsonDeserializer.TYPE_MAPPINGS,"uinfo:com.example.wrapper.userInfo");
        //  config.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        return config;
    }

   //Common consumer factory for usage by message listener container(s)
    @Bean
    @Primary
    public ConsumerFactory<String,String> consumerFactory()
    {
        // spring kafka's string deserialization wrapper with a purpose built parse function
        // i can also pass the parser function info as a property in the consumer factory
        return new DefaultKafkaConsumerFactory<>(config_src(),null , new ParseStringDeserializer<>((s,headers)->"faked deserialization"));
    }

    // Common consumer factory for usage by kafka listener container
    // factory value type is set to interface type
    // type mapping setup to pick the concrete implemntation of the interface for deserizlization
    @Bean
    public ConsumerFactory<String, objinf> consumerFactoryDup()
    {
        // using spring kafka json deserializer
        // producer has type info included and type mapping configured
        // trusting all packages for deserialization using properties
        // can also do this trusted deserilazation using programatic construction of deserializer
        // the below is unsafe as in the absence of type info , deserialization will fail since the type is abstract and not concrete

        return new DefaultKafkaConsumerFactory<>(config_src1(),null , new JsonDeserializer<>(objinf.class).trustedPackages("*"));
    }

    // used by the message listener container
    @Bean
    public ContainerProperties cprops()
    {
        // list of topics to consume from
        ContainerProperties cprops = new ContainerProperties("dummy");

        cprops.setLogContainerConfig(true);

        // commit is done immediately when Ack() is called inside message listener
        cprops.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // delivers polled records individually and an ack must be done after processing
        // the ack is what ultimately takes care of the commit
        cprops.setMessageListener(new AcknowledgingMessageListener<String,String>() {
            // this is where message consumption happens
            // Ack is needed given that we have disabled auto commit
            @Override
            public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
             System.out.println(data.value());
             acknowledgment.acknowledge();

            }
        });

        return cprops;
    }
    // message consumption based on message listener container
    // one container + one listener to listen/consume from all topics and their respective partitions as specified in the conatiner properties
    // to check -> another KMLC based on same containerproperties beans does use a separate message listener
    // the container as such is based on a single threaded principle where call to poll , message listener happens in same thread

    @Bean
    public KafkaMessageListenerContainer<String,String> listenerContainer(ConsumerFactory<String,String> csfact, ContainerProperties cprops)
    {
         KafkaMessageListenerContainer<String,String> container = new KafkaMessageListenerContainer<>(csfact,cprops);
         return container;
    }
    // message consumption based on concurrent message listener container
    // under the hood packs 1/many kafka message listener container based on concurrency set
    // the container as such is based on a single threaded principle where call to poll , message listener happens in same thread

    @Bean
    public ConcurrentMessageListenerContainer<String,String> concurrentContainer(ConsumerFactory<String,String> csfact , ContainerProperties cprops)
    {
        ConcurrentMessageListenerContainer<String , String> concurrentContainer = new ConcurrentMessageListenerContainer<>(csfact,cprops);
        concurrentContainer.setConcurrency(1);

        return concurrentContainer;

    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String,objinf>> kafkaListenerContainerFactory()
    {
        ConcurrentKafkaListenerContainerFactory<String, objinf> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactoryDup());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;

    }


}
