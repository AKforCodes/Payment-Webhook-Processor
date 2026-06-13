package com.payment.webhook.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

public class WebhookEvent {

    @NotBlank(message = "eventType is required")
    private String eventType;

    @NotNull(message = "amount is required")
    @Positive(message = "amount must be positive")
    private BigDecimal amount;

    @NotBlank(message = "currency is required")
    @Size(min = 3, max = 3, message = "currency must be a 3-letter code")
    private String currency;

    private String providerTransactionId;

    public WebhookEvent() {}

    public WebhookEvent(String eventType, BigDecimal amount, String currency,
                        String providerTransactionId) {
        this.eventType = eventType;
        this.amount = amount;
        this.currency = currency;
        this.providerTransactionId = providerTransactionId;
    }

    public String getEventType() { return eventType; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public String getProviderTransactionId() { return providerTransactionId; }

    public void setEventType(String eventType) { this.eventType = eventType; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public void setCurrency(String currency) { this.currency = currency; }
    public void setProviderTransactionId(String providerTransactionId) {
        this.providerTransactionId = providerTransactionId;
    }
}
