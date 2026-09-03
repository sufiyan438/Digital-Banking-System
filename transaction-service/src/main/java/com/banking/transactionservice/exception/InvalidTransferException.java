package com.banking.transactionservice.exception;

public class InvalidTransferException extends RuntimeException{
    public InvalidTransferException(String message){
        super(message);
    }
}
