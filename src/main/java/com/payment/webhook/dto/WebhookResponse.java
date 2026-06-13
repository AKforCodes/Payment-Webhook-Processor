package com.payment.webhook.dto;

import com.payment.webhook.entity.TransactionStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class WebhookResponse {

    private UUID id;
    private String eventType;
    private BigDecimal amount;
    private String currency;
    private TransactionStatus status;
    private String providerTransactionId;
    private Instant createdAt;

    public WebhookResponse() {}

    public WebhookResponse(UUID id, String eventType, BigDecimal amount,
                           String currency, TransactionStatus status,
                           String providerTransactionId, Instant createdAt) {
        this.id = id;
        this.eventType = eventType;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
        this.providerTransactionId = providerTransactionId;
        this.createdAt = createdAt;
    }

    public UUID getId() { return id; }
    public String getEventType() { return eventType; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public TransactionStatus getStatus() { return status; }
    public String getProviderTransactionId() { return providerTransactionId; }
    public Instant getCreatedAt() { return createdAt; }

    public void setId(UUID id) { this.id = id; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public void setCurrency(String currency) { this.currency = currency; }
    public void setStatus(TransactionStatus status) { this.status = status; }
    public void setProviderTransactionId(String providerTransactionId) { this.providerTransactionId = providerTransactionId; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
