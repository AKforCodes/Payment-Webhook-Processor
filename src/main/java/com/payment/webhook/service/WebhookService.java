package com.payment.webhook.service;

import com.payment.webhook.dto.WebhookEvent;
import com.payment.webhook.dto.WebhookResponse;
import com.payment.webhook.entity.PaymentTransaction;
import com.payment.webhook.kafka.WebhookEventProducer;
import com.payment.webhook.repository.PaymentTransactionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class WebhookService {

    private final PaymentTransactionRepository repository;
    private final WebhookEventProducer producer;

    public WebhookService(PaymentTransactionRepository repository,
                          WebhookEventProducer producer) {
        this.repository = repository;
        this.producer = producer;
    }

    @Transactional
    public WebhookResponse processWebhook(String idempotencyKey, WebhookEvent event) {
        var existing = repository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            return toResponse(existing.get());
        }

        PaymentTransaction transaction = new PaymentTransaction(
                idempotencyKey,
                event.getEventType(),
                event.getAmount(),
                event.getCurrency(),
                event.getProviderTransactionId()
        );

        transaction = repository.save(transaction);
        producer.publishEvent(transaction);

        return toResponse(transaction);
    }

    private WebhookResponse toResponse(PaymentTransaction tx) {
        return new WebhookResponse(
                tx.getId(),
                tx.getEventType(),
                tx.getAmount(),
                tx.getCurrency(),
                tx.getStatus(),
                tx.getProviderTransactionId(),
                tx.getCreatedAt()
        );
    }
}
