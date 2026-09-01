package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.exception.AccountServiceUnavailableException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountClientService {
    private final AccountServiceClient accountServiceClient;

    @CircuitBreaker(name = "accountService", fallbackMethod = "deductBalanceFallback")
    public void deductBalance(String accountNumber, BigDecimal amount) {
        accountServiceClient.deductBalance(accountNumber, amount);
    }

    private void deductBalanceFallback(String accountNumber, BigDecimal amount, Throwable throwable) {
        throw new AccountServiceUnavailableException("Account service unavailable while debiting account", throwable);
    }

    @Retry(name = "accountBalanceRetry", fallbackMethod = "getBalanceFallback")
    public BigDecimal getBalance(String accountNumber) {
        log.info("Calling account-service to fetch balance for account {}", accountNumber);
        return accountServiceClient.getBalance(accountNumber);
    }

    private BigDecimal getBalanceFallback(String accountNumber, Throwable throwable) {
        throw new AccountServiceUnavailableException("Unable to fetch account balance after retries",
                throwable
        );
    }

    @CircuitBreaker(
            name = "accountService",
            fallbackMethod = "creditBalanceFallback"
    )
    public void creditBalance(String accountNumber, BigDecimal amount) {
        accountServiceClient.creditBalance(accountNumber, amount);
    }

    private void creditBalanceFallback(
            String accountNumber,
            BigDecimal amount,
            Throwable throwable
    ) {
        throw new AccountServiceUnavailableException(
                "Account service unavailable while crediting account",
                throwable
        );
    }


    @CircuitBreaker(name = "accountService", fallbackMethod = "refundBalanceFallback")
    public void refundBalance(String transactionId, String accountNumber, BigDecimal amount) {
        accountServiceClient.refundBalance(accountNumber, transactionId, amount);
    }

    private void refundBalanceFallback(
            String transactionId,
            String accountNumber,
            BigDecimal amount,
            Throwable throwable) {
        throw new AccountServiceUnavailableException("Account service unavailable while refunding account", throwable);
    }
}