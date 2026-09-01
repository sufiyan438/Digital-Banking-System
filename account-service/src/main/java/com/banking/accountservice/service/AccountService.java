package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.exception.AccountNotFoundException;
import com.banking.accountservice.model.Account;
import com.banking.accountservice.model.AccountStatus;
import com.banking.accountservice.model.AccountType;
import com.banking.accountservice.model.OutboxEvent;
import com.banking.accountservice.repository.AccountRepository;
import com.banking.accountservice.repository.ProcessedEventRepository;
import com.banking.accountservice.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
//    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

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

//    public void deductBalance(String accountNumber, BigDecimal amount){
//        log.info("Deducting {} from account: {}", amount, accountNumber);
//        Account account = findByAccountNumber(accountNumber);
//        if(account.getAccountStatus() != AccountStatus.ACTIVE){
//            throw new RuntimeException("Account " + account.getAccountNumber() + " is blocked");
//        }
//        if(account.getBalance().compareTo(amount) < 0){
//            throw new RuntimeException("Insufficient balance");
//        }
//        account.setBalance(account.getBalance().subtract(amount));
//        accountRepository.save(account);
//        log.info("Amount deducted. New balance: {}", account.getBalance());
//    }

    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting {} from account: {}", amount, accountNumber);

        Account account = findByAccountNumber(accountNumber);

        if (account.getAccountStatus() != AccountStatus.ACTIVE) {
            throw new RuntimeException(
                    "Account " + account.getAccountNumber() + " is blocked"
            );
        }

        int updated = accountRepository.deductBalanceAtomic(
                accountNumber,
                amount
        );

        if (updated == 0) {
            throw new RuntimeException("Insufficient balance");
        }

        log.info(
                "Amount deducted successfully from account: {}",
                accountNumber
        );
    }

//    public void creditBalance(String accountNumber, BigDecimal amount){
//        log.info("Crediting {} to accout {}", amount, accountNumber);
//        Account account = findByAccountNumber(accountNumber);
//        account.setBalance(account.getBalance().add(amount));
//        accountRepository.save(account);
//        log.info("Amount credited. New balance: {}", account.getBalance());
//    }

    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Crediting {} to account {}", amount, accountNumber);

        int updated = accountRepository.creditBalanceAtomic(
                accountNumber,
                amount
        );

        if (updated == 0) {
            throw new AccountNotFoundException(
                    "Account not found: " + accountNumber
            );
        }

        log.info(
                "Amount credited successfully to account: {}",
                accountNumber
        );
    }

    private String generateAccountNumber(){
        String accountNumber;
        do{
            long number = secureRandom.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d", number);
        }while(accountRepository.existsByAccountNumber(accountNumber));
        return accountNumber;
    }

    @Transactional
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String, Object> payload){
//        try {
//            String receiverAccount = (String) payload.get("receiverAccountNumber");
//            BigDecimal amount = new BigDecimal(payload.get("amount").toString());
//            log.info("Crediting account: {} amount: {}", receiverAccount, amount);
//            creditBalance(receiverAccount, amount);
//        } catch (Exception e) {
//            log.error("Error crediting account. Error: {}", e.getMessage());
//        }
        String transactionId = payload.get("transactionId").toString();

        String eventId = "transaction-completed-" + transactionId;

        int inserted = processedEventRepository.insertIfAbsent(
                eventId,
                "TRANSACTION_COMPLETED"
        );

        if (inserted == 0) {
            log.warn(
                    "Duplicate transaction.completed event ignored for transaction {}",
                    transactionId
            );
            return;
        }

        String receiverAccount = payload.get("receiverAccountNumber").toString();
        BigDecimal amount = new BigDecimal(payload.get("amount").toString());
        try {

            log.info(
                    "Crediting account: {} amount: {}",
                    receiverAccount,
                    amount
            );

            creditBalance(receiverAccount, amount);
            Map<String, Object> successEvent = new HashMap<>();

            successEvent.put("transactionId", transactionId);

            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateId(transactionId)
                    .eventType("TRANSACTION_CREDIT_SUCCEEDED")
                    .topic("transaction.credit.succeeded")
                    .payload(objectMapper.writeValueAsString(successEvent))
                    .status("PENDING")
                    .createdAt(LocalDateTime.now())
                    .build();

            outboxEventRepository.save(outboxEvent);

//            kafkaTemplate.send(
//                    "transaction.credit.succeeded",
//                    transactionId,
//                    successEvent
//            );

        } catch (AccountNotFoundException e) {
            log.error(
                    "Credit failed for transaction {}. Error: {}",
                    transactionId,e.getMessage());

            Map<String, Object> failureEvent = new HashMap<>();

            failureEvent.put("transactionId", transactionId);
            failureEvent.put("receiverAccountNumber", receiverAccount);
            failureEvent.put("amount", amount);
            failureEvent.put("reason", e.getMessage());

//            kafkaTemplate.send(
//                    "transaction.credit.failed",
//                    transactionId,
//                    failureEvent
//            );
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateId(transactionId)
                    .eventType("TRANSACTION_CREDIT_FAILED")
                    .topic("transaction.credit.failed")
                    .payload(objectMapper.writeValueAsString(failureEvent))
                    .status("PENDING")
                    .createdAt(LocalDateTime.now())
                    .build();

            outboxEventRepository.save(outboxEvent);
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
