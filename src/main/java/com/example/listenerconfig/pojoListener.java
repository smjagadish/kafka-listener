package com.example.listenerconfig;

import com.example.wrapper.userInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.converter.KafkaMessageHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.ParseStringDeserializer;
import org.springframework.kafka.support.serializer.ToStringSerializer;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Component

public class pojoListener {
    // batch listener
    //consumer group id is overridden
    @KafkaListener(topics = "sample-topic4",autoStartup = "${listen.auto.start:true}",groupId = "new-listener",batch = "true")
    public void listen(List<String> data , Acknowledgment ack ,@Header(KafkaHeaders.OFFSET) List<Long> offsets)
    {
        String cons_id = KafkaUtils.getConsumerGroupId();
        System.out.println(cons_id.toString());
        Iterator it = data.iterator();
        while (it.hasNext()) {

            System.out.println(it.next());

        }

      ack.acknowledge();
    }


    /*ParseStringDeserializer<String> obj = new ParseStringDeserializer<>((str,headers)->{
        return str;

    });*/

    //individual listener

    @KafkaListener(topics = "sample-topic4",autoStartup = "${listen.auto.start:true}",groupId = "delta-listener")
    public void listen_rec(userInfo data , Acknowledgment ack , ConsumerRecordMetadata cmd , @Headers Map<String,Object> header)
    {
        System.out.println(data.getId());
        System.out.println(cmd.topic()+" with partition"+cmd.partition()+"with offset"+cmd.offset());

        ack.acknowledge();
    }


}
