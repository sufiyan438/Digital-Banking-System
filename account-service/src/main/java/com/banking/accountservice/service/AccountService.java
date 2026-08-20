package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.model.Account;
import com.banking.accountservice.model.AccountStatus;
import com.banking.accountservice.model.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.util.Map;

@Slf4j
@Service
public class AccountService {

    @Autowired
    private AccountRepository accountRepository;

    private static SecureRandom secureRandom = new SecureRandom();

    public AccountResponse createAccount(CreateAccountRequest request){
        log.info("Creating account for {}", request.getEmail());
        if(accountRepository.existsByEmail(request.getEmail())){
            throw new RuntimeException("Account already exists for this mail: " + request.getEmail());
        }
        Account account = Account.builder()
                .accountNumber(generateAccountNumber())
                .accountHolderName(request.getAccountHolderName())
                .email(request.getEmail())
                .phone(request.getPhone())
                .accountStatus(AccountStatus.ACTIVE)
                .accountType(request.getAccountType())
                .balance(request.getInitialDeposit())
                .dailyTransactionLimit(request.getAccountType() == AccountType.SAVINGS
                    ? new BigDecimal("100000")
                    : new BigDecimal("500000"))
                .build();

        Account savedAccount = accountRepository.save(account);
        log.info("Account created: {}", savedAccount.getAccountNumber());
        return mapToResponse(savedAccount);
    }

    private AccountResponse mapToResponse(Account account){
        AccountResponse response = AccountResponse.builder()
                .id(account.getId())
                .accountNumber(account.getAccountNumber())
                .accountHolderName(account.getAccountHolderName())
                .email(account.getEmail())
                .phone(account.getPhone())
                .accountStatus(account.getAccountStatus())
                .accountType(account.getAccountType())
                .balance(account.getBalance())
                .dailyTransactionLimit(account.getDailyTransactionLimit())
                .createdAt(account.getCreatedAt())
                .build();
        return response;
    }

    public AccountResponse getAccount(String accountNumber){
        Account account = findByAccountNumber(accountNumber);
        return mapToResponse(account);
    }

    private Account findByAccountNumber(String accountNumber){
        return accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found: " + accountNumber));
    }

    public BigDecimal getBalance(String accountNumber){
        return findByAccountNumber(accountNumber).getBalance();
    }

    public void blockAccount(String accountNumber){
        log.info("Blocking account: {}", accountNumber);
        Account account = findByAccountNumber(accountNumber);
        account.setAccountStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account bearing number {} has been blocked", accountNumber);
    }

    public void deductBalance(String accountNumber, BigDecimal amount){
        log.info("Deducting {} from account: {}", amount, accountNumber);
        Account account = findByAccountNumber(accountNumber);
        if(account.getAccountStatus() != AccountStatus.ACTIVE){
            throw new RuntimeException("Account " + account.getAccountNumber() + " is blocked");
        }
        if(account.getBalance().compareTo(amount) < 0){
            throw new RuntimeException("Insufficient balance");
        }
        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.info("Amount deducted. New balance: {}", account.getBalance());
    }

    public void creditBalance(String accountNumber, BigDecimal amount){
        log.info("Crediting {} to accout {}", amount, accountNumber);
        Account account = findByAccountNumber(accountNumber);
        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Amount credited. New balance: {}", account.getBalance());
    }

    private String generateAccountNumber(){
        String accountNumber;
        do{
            long number = secureRandom.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d", number);
        }while(accountRepository.existsByAccountNumber(accountNumber));
        return accountNumber;
    }

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String, Object> payload){
        try {
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());
            log.info("Crediting account: {} amount: {}", receiverAccount, amount);
            creditBalance(receiverAccount, amount);
        } catch (Exception e) {
            log.error("Error crediting account. Error: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String, Object> payload){
        try {
            String accountNumber = payload.get("accountNumber").toString();
            log.info("Fraud detected. Blocking account numebr: {}", accountNumber);
            blockAccount(accountNumber);
        } catch (Exception e) {
            log.info("Error blocking account. Error: {}", e.getMessage());
        }
    }
}
