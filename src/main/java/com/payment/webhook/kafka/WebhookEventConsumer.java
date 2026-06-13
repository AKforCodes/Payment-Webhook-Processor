package com.payment.webhook.kafka;

import com.payment.webhook.entity.PaymentTransaction;
import com.payment.webhook.entity.TransactionStatus;
import com.payment.webhook.repository.PaymentTransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class WebhookEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(WebhookEventConsumer.class);

    private final PaymentTransactionRepository repository;

    public WebhookEventConsumer(PaymentTransactionRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "${payment.webhook.topic}",
                   groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void consume(PaymentTransaction transaction) {
        log.info("Processing event for txn {} type {}",
                transaction.getId(), transaction.getEventType());

        var existing = repository.findById(transaction.getId());
        if (existing.isEmpty()) {
            log.warn("Transaction {} not found in DB, skipping", transaction.getId());
            return;
        }

        var tx = existing.get();
        switch (tx.getEventType()) {
            case "payment.intent.succeeded" -> tx.setStatus(TransactionStatus.SUCCEEDED);
            case "payment.intent.failed" -> tx.setStatus(TransactionStatus.FAILED);
            case "charge.refunded" -> tx.setStatus(TransactionStatus.REFUNDED);
            case "charge.refund.voided" -> tx.setStatus(TransactionStatus.VOIDED);
            default -> log.warn("Unknown event type: {}", tx.getEventType());
        }

        repository.save(tx);
        log.info("Transaction {} updated to {}", tx.getId(), tx.getStatus());
    }
}
