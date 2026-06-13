package com.payment.webhook.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payment_transactions",
       uniqueConstraints = @UniqueConstraint(columnNames = "idempotency_key"))
public class PaymentTransaction {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TransactionStatus status;

    @Column(name = "provider_transaction_id")
    private String providerTransactionId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
        updatedAt = Instant.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }

    public PaymentTransaction() {}

    public PaymentTransaction(String idempotencyKey, String eventType,
                              BigDecimal amount, String currency,
                              String providerTransactionId) {
        this.idempotencyKey = idempotencyKey;
        this.eventType = eventType;
        this.amount = amount;
        this.currency = currency;
        this.providerTransactionId = providerTransactionId;
        this.status = TransactionStatus.PROCESSING;
    }

    public UUID getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getEventType() { return eventType; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public TransactionStatus getStatus() { return status; }
    public String getProviderTransactionId() { return providerTransactionId; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    public void setStatus(TransactionStatus status) { this.status = status; }
}
