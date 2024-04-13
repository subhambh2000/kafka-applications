package com.kafka.spring;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    @KafkaListener(topics = "test",containerFactory = "concurrentKafkaListenerContainerFactory")
    public void consumer(String message){
        System.out.println("Filtered Message: " + message);
    }

}