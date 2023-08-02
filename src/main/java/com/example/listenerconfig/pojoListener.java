package com.example.listenerconfig;

import com.example.extraMapper.empInfo;
import com.example.mapinterface.objinf;
import com.example.wrapper.userInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.converter.KafkaMessageHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.ParseStringDeserializer;
import org.springframework.kafka.support.serializer.ToStringSerializer;
import org.springframework.messaging.MessageHeaders;
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
    public void listen(List<objinf> data , Acknowledgment ack , @Header(KafkaHeaders.OFFSET) List<Long> offsets)
    {
        String cons_id = KafkaUtils.getConsumerGroupId();
        System.out.println(cons_id.toString());
        //Jul 10 , 2023 changes to throw batch listener exception when encountering certain record types
        // this is for sake of understanding and wont actually get triggered
        // intent is to let know the listener from which record in batch the processing failed so that it can be re-tried during next fetch
        int index =0;
        Iterator it = data.iterator();
        while (it.hasNext()) {
           objinf object = (objinf)it.next();
           if(object!=null) {
               System.out.println(object.getdept());
               if(object.getdept().equals("CSE"))
               throw new BatchListenerFailedException("dummy exception",index);
               else
                   index++;
           }
           else {
               System.out.println("error handling deser passed down the puck");
               index++;
           }

        }

      ack.acknowledge();
    }


    /*ParseStringDeserializer<String> obj = new ParseStringDeserializer<>((str,headers)->{
        return str;

    });*/

    //individual listener

    @KafkaListener(topics = "sample-topic4",autoStartup = "${listen.auto.start:true}",groupId = "delta-listener")
    public void listen_rec(objinf data , Acknowledgment ack , ConsumerRecordMetadata cmd , @Headers Map<String,Object> header , @Headers MessageHeaders msgheaders)
    {
        System.out.println(data.getid());
        System.out.println(cmd.topic()+" with partition"+cmd.partition()+"with offset"+cmd.offset());

        ack.acknowledge();
    }


}
