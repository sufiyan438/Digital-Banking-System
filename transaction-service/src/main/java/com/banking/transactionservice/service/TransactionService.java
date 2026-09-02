package com.banking.transactionservice.service;

import com.banking.transactionservice.exception.OutboxCreationException;
import com.banking.transactionservice.exception.TransactionNotFoundException;
import com.banking.transactionservice.model.OutboxEvent;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;
import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.model.Transaction;
import com.banking.transactionservice.model.TransactionStatus;
import com.banking.transactionservice.model.TransactionType;
import com.banking.transactionservice.repository.OutboxEventRepository;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final AccountServiceClient accountServiceClient;
    private final AccountClientService accountClientService;

    private final ObjectMapper objectMapper;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    @Transactional
    public TransactionResponse transfer(TransferRequest request){
        log.info("SAGA pattern kicks in here. Transferring INR {} from {} to {}",
                request.getAmount(), request.getSenderAccountNumber(), request.getRecieverAccountNumber());

        accountClientService.deductBalance(request.getSenderAccountNumber(), request.getAmount());
        log.info(
                "Sender debit successful. Account: {}, Amount: {}",
                request.getSenderAccountNumber(), request.getAmount()
        );


        Transaction transaction = Transaction.builder()
                .senderAccountNumber(request.getSenderAccountNumber())
                .receiverAccountNumber(request.getRecieverAccountNumber())
                .amount(request.getAmount())
                .type(TransactionType.TRANSFER)
                .status(TransactionStatus.PROCESSING)
                .description(request.getDescription())
                .referencedNumber(UUID.randomUUID().toString())
                .build();

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Saved transaction as PROCESSING. Transaction ID: {}", savedTransaction.getId());

        TransactionInitiatedEvent event = TransactionInitiatedEvent.builder()
                .transactionId(savedTransaction.getId())
                .senderAccountNumber(savedTransaction.getSenderAccountNumber())
                .receiverAccountNumber(savedTransaction.getReceiverAccountNumber())
                .amount(savedTransaction.getAmount())
                .description(savedTransaction.getDescription())
                .build();

//        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
        try {

            String payload = objectMapper.writeValueAsString(event);

            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(UUID.randomUUID().toString())
                    .aggregateId(transaction.getId())
                    .eventType("TRANSACTION_INITIATED")
                    .topic("transaction.initiated")
                    .payload(payload)
                    .status("PENDING")
                    .createdAt(LocalDateTime.now())
                    .build();

            outboxEventRepository.save(outboxEvent);

            log.info(
                    "TransactionInitiatedEvent stored in outbox: {}",
                    transaction.getId()
            );

        } catch (Exception e) {

            throw new OutboxCreationException(
                    "Failed to create outbox event",
                    e
            );
        }
        log.info("SAGA STEP 2 - TransactionInitiatedEvent queued in outbox: {}", savedTransaction.getId());
        return mapToResponse(savedTransaction);
    }

    private TransactionResponse mapToResponse(Transaction transaction){
        TransactionResponse response = TransactionResponse.builder()
                .id(transaction.getId())
                .senderAccountNumber(transaction.getSenderAccountNumber())
                .receiverAccountNumber(transaction.getReceiverAccountNumber())
                .amount(transaction.getAmount())
                .type(transaction.getType())
                .status(transaction.getStatus())
                .description(transaction.getDescription())
                .failureReason(transaction.getFailureReason())
                .referencedNumber(transaction.getReferencedNumber())
                .createdAt(transaction.getCreatedAt())
                .completedAt(transaction.getCompletedAt())
                .build();
        return response;
    }

    public TransactionResponse getTransaction(String transactionId){
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() ->
                        new TransactionNotFoundException("Transaction " + transactionId +" not found"));
        return mapToResponse(transaction);
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber){
        List<Transaction> list = transactionRepository.findAllBySenderAccountNumberOrderByCreatedAtDesc(accountNumber);
        return list.stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    @Transactional
    public TransactionResponse verifyOtp(String transactionId, String otp){
        log.info("OTP verification for the transaction: {}", transactionId);
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new TransactionNotFoundException("Transaction " + transactionId +" not found"));
        String otpKey = "verification:otp" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null){
            //OTP expired
            log.warn("OTP has expired for transaction {}", transactionId);

            int updated = transactionRepository.updateStatusIfCurrent(
                    transactionId,
                    TransactionStatus.PENDING_VERIFICATION,
                    TransactionStatus.COMPENSATING
            );

            if (updated == 0) {
                log.warn(
                        "Transaction {} is no longer PENDING_VERIFICATION. Skipping compensation.",
                        transactionId
                );
                return mapToResponse(transaction);
            }

            compensateTransaction(transaction, "OTP expired. Transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }


        if(!storedOtp.equals(otp)){
            //Block account and refund
            log.warn("Wrong OTP. Blocking account and refunding money for transaction: {}", transactionId);

            int updated = transactionRepository.updateStatusIfCurrent(
                    transactionId,
                    TransactionStatus.PENDING_VERIFICATION,
                    TransactionStatus.COMPENSATING
            );

            if (updated == 0) {
                log.warn(
                        "Transaction {} is no longer PENDING_VERIFICATION. Skipping compensation.",
                        transactionId
                );

                return mapToResponse(transaction);
            }

            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction, "Wrong OTP entered. Transaction cancelled and account blocked. Contact bank for further resolution.");
            return mapToResponse(transaction);
        }
        //Correct OTP
//        log.info("OTP verified. Compeleting transaction {}", transaction);
//        redisTemplate.delete(otpKey);
//        completeTransaction(transaction);
//        return mapToResponse(transaction);


        /*Doing this to ensure that race condition does not happen
        i.e.user enters OTP at end time and scheduler sees it as expired. hence adding this concurrency guard
        */

        // Correct OTP
        log.info("OTP verified. Completing transaction {}", transactionId);

        int updated = transactionRepository.updateStatusIfCurrent(
                transactionId,
                TransactionStatus.PENDING_VERIFICATION,
                TransactionStatus.CREDIT_PENDING
        );

        if (updated == 0) {
            log.warn(
                    "Transaction {} is no longer PENDING_VERIFICATION. Skipping completion.",
                    transactionId
            );

            Transaction latest = transactionRepository.findById(transactionId)
                    .orElse(transaction);

            return mapToResponse(latest);
        }

        transaction.setStatus(TransactionStatus.CREDIT_PENDING);
        redisTemplate.delete(otpKey);

        completeTransaction(transaction);

        return mapToResponse(transaction);
    }

    private void compensateTransaction(Transaction transaction, String reason){
        log.warn("SAGA compensation - refunding to: {} amount: {}", transaction.getSenderAccountNumber(), transaction.getAmount());

        //Credit money back then publish event to Kafka which will notify user
//        accountServiceClient.creditBalance(transaction.getSenderAccountNumber(), transaction.getAmount());


//        accountClientService.creditBalance(
//                transaction.getSenderAccountNumber(),
//                transaction.getAmount()
//        );

        accountClientService.refundBalance(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getAmount()
        );


//        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setStatus(TransactionStatus.REFUNDED);
        transaction.setFailureReason(reason + "SAGA compensation executed. Amount refunded at " + LocalDateTime.now());
        transactionRepository.save(transaction);

        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);
        log.info("SAGA compensation compeleted. {} refunded to {}", transaction.getAmount(), transaction.getSenderAccountNumber());
    }

    private void blockAccountAndCompensate(Transaction transaction, String reason){
        Map<String, Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason", reason);
        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);
        log.warn("fraud.detected published - account: {} will be blocked. Kindly contact the bank",
                transaction.getSenderAccountNumber());
        compensateTransaction(transaction, reason);
    }

    private void completeTransaction(Transaction transaction){
//        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setStatus(TransactionStatus.CREDIT_PENDING);
//        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = TransactionCompletedEvent.builder()
                .transactionId(transaction.getId())
                .senderAccountNumber(transaction.getSenderAccountNumber())
                .receiverAccountNumber(transaction.getReceiverAccountNumber())
                .amount(transaction.getAmount())
                .description(transaction.getDescription())
                .build();

//        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);
//        log.info("SAGA Complete. Transaction {} completed!", transaction.getId());
        try {
            String payload = objectMapper.writeValueAsString(completedEvent);

            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(UUID.randomUUID().toString())
                    .aggregateId(transaction.getId())
                    .eventType("TRANSACTION_COMPLETED")
                    .topic(TRANSACTION_COMPLETED_TOPIC)
                    .payload(payload)
                    .status("PENDING")
                    .createdAt(LocalDateTime.now())
                    .build();

            outboxEventRepository.save(outboxEvent);

        } catch (Exception e) {
            throw new OutboxCreationException(
                    "Failed to create transaction completed outbox event",
                    e
            );
        }
        log.info("Transaction {} awaiting receiver credit", transaction.getId());
    }

    @Transactional
    public void processCleanResult(String transactionId){
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new TransactionNotFoundException("Transaction " + transactionId +" not found"));
        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction {} not PROCESSING - skipping", transactionId);
            return;
        }
        completeTransaction(transaction);
    }


    @Scheduled(fixedRate = 60000)
    @Transactional
    public void handleExpiredOtps() {

        List<Transaction> pendingTransactions =
                transactionRepository.findAllByStatus(
                        TransactionStatus.PENDING_VERIFICATION
                );

        for (Transaction transaction : pendingTransactions) {

            String otpKey = "verification:otp" + transaction.getId();
            String storedOtp = redisTemplate.opsForValue().get(otpKey);

            if (storedOtp == null) {

                log.warn(
                        "OTP expired automatically for transaction {}",
                        transaction.getId()
                );

                int updated = transactionRepository.updateStatusIfCurrent(
                        transaction.getId(),
                        TransactionStatus.PENDING_VERIFICATION,
                        TransactionStatus.COMPENSATING
                );

                if (updated == 1) {
                    compensateTransaction(
                            transaction,
                            "OTP expired. Transaction cancelled and amount refunded. "
                    );
                }
            }
        }
    }

}
