package com.payment.webhook.kafka;

import com.payment.webhook.entity.PaymentTransaction;
import com.payment.webhook.entity.TransactionStatus;
import com.payment.webhook.repository.PaymentTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WebhookEventConsumerTest {

    @Mock
    private PaymentTransactionRepository repository;

    private WebhookEventConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new WebhookEventConsumer(repository);
    }

    private PaymentTransaction createTransaction(String eventType) {
        PaymentTransaction tx = new PaymentTransaction(
                UUID.randomUUID().toString(), eventType,
                new BigDecimal("1000"), "USD", "pi_test"
        );
        try {
            var idField = PaymentTransaction.class.getDeclaredField("id");
            idField.setAccessible(true);
            idField.set(tx, UUID.randomUUID());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return tx;
    }

    @Test
    void shouldUpdateToSucceededForPaymentSucceeded() {
        PaymentTransaction tx = createTransaction("payment.intent.succeeded");
        when(repository.findById(tx.getId())).thenReturn(Optional.of(tx));
        consumer.consume(tx);
        verify(repository).save(tx);
        assertThat(tx.getStatus()).isEqualTo(TransactionStatus.SUCCEEDED);
    }

    @Test
    void shouldUpdateToFailedForPaymentFailed() {
        PaymentTransaction tx = createTransaction("payment.intent.failed");
        when(repository.findById(tx.getId())).thenReturn(Optional.of(tx));
        consumer.consume(tx);
        verify(repository).save(tx);
        assertThat(tx.getStatus()).isEqualTo(TransactionStatus.FAILED);
    }

    @Test
    void shouldUpdateToRefundedForChargeRefunded() {
        PaymentTransaction tx = createTransaction("charge.refunded");
        when(repository.findById(tx.getId())).thenReturn(Optional.of(tx));
        consumer.consume(tx);
        verify(repository).save(tx);
        assertThat(tx.getStatus()).isEqualTo(TransactionStatus.REFUNDED);
    }

    @Test
    void shouldUpdateToVoidedForRefundVoided() {
        PaymentTransaction tx = createTransaction("charge.refund.voided");
        when(repository.findById(tx.getId())).thenReturn(Optional.of(tx));
        consumer.consume(tx);
        verify(repository).save(tx);
        assertThat(tx.getStatus()).isEqualTo(TransactionStatus.VOIDED);
    }

    @Test
    void shouldSkipWhenTransactionNotFound() {
        PaymentTransaction tx = createTransaction("payment.intent.succeeded");
        when(repository.findById(tx.getId())).thenReturn(Optional.empty());
        consumer.consume(tx);
        verify(repository, never()).save(any());
    }
}
