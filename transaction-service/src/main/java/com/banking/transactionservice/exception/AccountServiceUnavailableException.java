package com.banking.transactionservice.exception;

public class AccountServiceUnavailableException extends RuntimeException{
    public AccountServiceUnavailableException(String message, Throwable cause){
        super(message, cause);
    }
}
