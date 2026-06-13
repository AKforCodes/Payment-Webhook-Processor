package com.payment.webhook.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Value("${payment.webhook.topic}")
    private String topic;

    @Bean
    public NewTopic paymentEventsTopic() {
        return new NewTopic(topic, 1, (short) 1);
    }
}
