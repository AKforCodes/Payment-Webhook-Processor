package com.payment.webhook.kafka;

import com.payment.webhook.entity.PaymentTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class WebhookEventProducer {

    private static final Logger log = LoggerFactory.getLogger(WebhookEventProducer.class);

    private final KafkaTemplate<String, PaymentTransaction> kafkaTemplate;
    private final String topic;

    public WebhookEventProducer(KafkaTemplate<String, PaymentTransaction> kafkaTemplate,
                                @Value("${payment.webhook.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publishEvent(PaymentTransaction transaction) {
        CompletableFuture<SendResult<String, PaymentTransaction>> future =
                kafkaTemplate.send(topic, transaction.getIdempotencyKey(), transaction);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send event for txn {}: {}",
                        transaction.getId(), ex.getMessage(), ex);
            } else {
                log.info("Published event for txn {} to partition {} offset {}",
                        transaction.getId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
