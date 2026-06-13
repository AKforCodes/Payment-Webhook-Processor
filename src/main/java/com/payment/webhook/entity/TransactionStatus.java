package com.payment.webhook.entity;

public enum TransactionStatus {
    PROCESSING,
    SUCCEEDED,
    FAILED,
    REFUNDED,
    VOIDED
}
