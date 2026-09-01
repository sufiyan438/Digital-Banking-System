package com.banking.transactionservice.exception;

public class OutboxCreationException extends RuntimeException{
    public OutboxCreationException(String message, Throwable cause){
        super(message, cause);
    }
}
