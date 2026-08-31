package com.banking.transactionservice.model;

public enum TransactionStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    PENDING_VERIFICATION,
    CREDIT_PENDING,
    REFUNDED,
    COMPENSATING,
    FAILED,
    FLAGGED
}
