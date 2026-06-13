package com.payment.webhook.service;

import com.payment.webhook.dto.WebhookEvent;
import com.payment.webhook.dto.WebhookResponse;
import com.payment.webhook.entity.PaymentTransaction;
import com.payment.webhook.entity.TransactionStatus;
import com.payment.webhook.kafka.WebhookEventProducer;
import com.payment.webhook.repository.PaymentTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WebhookServiceTest {

    @Mock
    private PaymentTransactionRepository repository;

    @Mock
    private WebhookEventProducer producer;

    private WebhookService service;

    @BeforeEach
    void setUp() {
        service = new WebhookService(repository, producer);
    }

    @Test
    void shouldCreateTransactionAndPublishForNewIdempotencyKey() {
        String idempotencyKey = UUID.randomUUID().toString();
        WebhookEvent event = new WebhookEvent(
                "payment.intent.succeeded", new BigDecimal("2999"), "USD", "pi_abc123"
        );

        when(repository.findByIdempotencyKey(idempotencyKey)).thenReturn(Optional.empty());
        when(repository.save(any(PaymentTransaction.class)))
                .thenAnswer(invocation -> {
                    PaymentTransaction tx = invocation.getArgument(0);
                    try {
                        var idField = PaymentTransaction.class.getDeclaredField("id");
                        idField.setAccessible(true);
                        idField.set(tx, UUID.randomUUID());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return tx;
                });

        WebhookResponse response = service.processWebhook(idempotencyKey, event);

        assertThat(response.getStatus()).isEqualTo(TransactionStatus.PROCESSING);
        assertThat(response.getEventType()).isEqualTo("payment.intent.succeeded");
        assertThat(response.getAmount()).isEqualByComparingTo(new BigDecimal("2999"));

        ArgumentCaptor<PaymentTransaction> captor = ArgumentCaptor.forClass(PaymentTransaction.class);
        verify(repository).save(captor.capture());
        assertThat(captor.getValue().getIdempotencyKey()).isEqualTo(idempotencyKey);

        verify(producer).publishEvent(captor.capture());
    }

    @Test
    void shouldReturnExistingTransactionForDuplicateIdempotencyKey() {
        String idempotencyKey = UUID.randomUUID().toString();
        UUID id = UUID.randomUUID();
        PaymentTransaction existing = new PaymentTransaction(
                idempotencyKey, "payment.intent.succeeded",
                new BigDecimal("2999"), "USD", "pi_abc123"
        );
        try {
            var idField = PaymentTransaction.class.getDeclaredField("id");
            idField.setAccessible(true);
            idField.set(existing, id);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        when(repository.findByIdempotencyKey(idempotencyKey)).thenReturn(Optional.of(existing));

        WebhookEvent event = new WebhookEvent(
                "payment.intent.succeeded", new BigDecimal("2999"), "USD", "pi_abc123"
        );

        WebhookResponse response = service.processWebhook(idempotencyKey, event);

        assertThat(response.getId()).isEqualTo(id);
        verify(repository, never()).save(any());
        verify(producer, never()).publishEvent(any());
    }
}
