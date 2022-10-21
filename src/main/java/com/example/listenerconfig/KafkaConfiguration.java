package com.example.listenerconfig;

import com.example.wrapper.userInfo;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.protocol.types.Field;
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

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC;

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
        config.put(VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        config.put(GROUP_ID_CONFIG,"process-1");
        config.put(ENABLE_AUTO_COMMIT_CONFIG,false);
        config.put(JsonDeserializer.TRUSTED_PACKAGES,"*");
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
    @Bean
    public ConsumerFactory<String,userInfo> consumerFactoryDup()
    {
        // using spring kafka json deserializer
        // producer has type info disabled
        // trusting all packages for deserialization
        // can also do this trusted deserialization using properties or in the factory properties

        return new DefaultKafkaConsumerFactory<>(config_src1(), null,new JsonDeserializer<>(userInfo.class).trustedPackages("*"));
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
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String,userInfo>> kafkaListenerContainerFactory()
    {
        ConcurrentKafkaListenerContainerFactory<String, userInfo> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactoryDup());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;
    }


}
