package com.banking.transactionservice.service;
import com.banking.transactionservice.model.ProcessedEvent;

import io.micrometer.core.instrument.MeterRegistry;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.model.Transaction;
import com.banking.transactionservice.model.TransactionStatus;
import com.banking.transactionservice.repository.ProcessedEventRepository;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {

    private final TransactionRepository transactionRepository;
    private final ProcessedEventRepository processedEventRepository;

    private final AccountServiceClient accountServiceClient;
    private final AccountClientService accountClientService;

    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final TransactionService transactionService;

    private final MeterRegistry meterRegistry;

    private static final String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";
    private static final long OTP_EXPIRY_MINUTES = 5;

    @KafkaListener(topics = "verification.required", groupId = "transaction-service-group")
    public void consumeVerificationRequired(@Payload Map<String, Object> payload){
        try {
            String transactionId = (String) payload.get("transactionId");

            String eventId = "verification-required-" + transactionId;

            int inserted = processedEventRepository.insertIfAbsent(
                    eventId,
                    "VERIFICATION_REQUIRED"
            );

            if (inserted == 0) {
                log.warn(
                        "Duplicate verification required event ignored for transaction {}",
                        transactionId
                );
                return;
            }

            String accountNumber = (String) payload.get("accountNumber");
            String reason = payload.get("reason").toString();
            log.info("Verification required for transactionIf: {} and reason: {}", transactionId, reason);

            Transaction transaction = transactionRepository.findById(transactionId)
                    .orElseThrow(() -> new RuntimeException("Transaction " + transactionId + " not found"));

            if(transaction.getStatus() != TransactionStatus.PROCESSING){
                log.warn("Transaction {} is not in PROCESSING status. Hence skipping", transactionId);
                return;
            }

            String otp = String.format("%06d", (int) (Math.random() * 900000) + 100000);
            String otpKey = "verification:otp" + transactionId;
            redisTemplate.opsForValue().set(otpKey, otp, OTP_EXPIRY_MINUTES, TimeUnit.MINUTES);

            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);
            log.info("OTP generated for transaction with id: {}. It expires in {} minutes", transactionId, OTP_EXPIRY_MINUTES);

            Map<String, Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId", transactionId);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("reason", reason);
            otpEvent.put("otp", otp);
            otpEvent.put("amount", payload.get("amount"));
            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC, transactionId, otpEvent);
        }
        catch (Exception e){
            log.error("Error handling verification required: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.check.clean", groupId = "transaction-service-group")
    public void consumeFraudCheckCleanResult(@Payload Map<String, Object> payload){
        try{
            String transactionId = (String) payload.get("transactionId");
            String eventId =
                    "fraud-check-clean-" + transactionId;

            int inserted =
                    processedEventRepository.insertIfAbsent(
                            eventId,
                            "FRAUD_CHECK_CLEAN"
                    );

            if (inserted == 0) {
                log.warn(
                        "Duplicate fraud check clean event ignored for transaction {}",
                        transactionId
                );
                return;
            }
            transactionService.processCleanResult(transactionId);
        }
        catch (Exception e){
            log.error("Error processing fraud check result {}", e.getMessage());
        }
    }

    @Transactional
    @KafkaListener(
            topics = "transaction.credit.failed",
            groupId = "transaction-service-group"
    )
    public void consumeCreditFailed(
            @Payload Map<String, Object> payload) {

        String transactionId = payload.get("transactionId").toString();
        String reason = payload.get("reason").toString();
        String eventId = "credit-failed-" + transactionId;

        int inserted = processedEventRepository.insertIfAbsent(
                eventId,
                "TRANSACTION_CREDIT_FAILED"
        );

        if (inserted == 0) {

            log.warn(
                    "Duplicate credit failure event ignored for transaction {}",
                    transactionId
            );

            return;
        }

        log.error(
                "Credit failure received for transaction {}. Reason: {}",
                transactionId,
                reason
        );

        int updated = transactionRepository.updateStatusIfCurrent(
                transactionId,
                TransactionStatus.CREDIT_PENDING,
                TransactionStatus.COMPENSATING
        );

        if (updated == 1) {
            meterRegistry
                    .counter("banking.compensation.triggered")
                    .increment();
            log.warn(
                    "Transaction {} moved to COMPENSATING",
                    transactionId
            );

            Transaction transaction = transactionRepository
                    .findById(transactionId)
                    .orElseThrow(() ->
                            new RuntimeException(
                                    "Transaction not found: " + transactionId
                            )
                    );

            try {

//                accountServiceClient.creditBalance(
//                        transaction.getSenderAccountNumber(),
//                        transaction.getAmount()
//                );
                accountClientService.creditBalance(
                        transaction.getSenderAccountNumber(),
                        transaction.getAmount()
                );

                int refundUpdated = transactionRepository.updateStatusIfCurrent(
                        transactionId,
                        TransactionStatus.COMPENSATING,
                        TransactionStatus.REFUNDED
                );

                if (refundUpdated == 1) {
                    meterRegistry
                            .counter("banking.compensation.succeeded")
                            .increment();

                    log.info(
                            "Compensation successful. Transaction {} moved to REFUNDED",
                            transactionId
                    );

                } else {

                    log.error(
                            "Refund succeeded but transaction {} could NOT move from COMPENSATING to REFUNDED",
                            transactionId
                    );
                }

            } catch (Exception e) {
                meterRegistry
                        .counter("banking.compensation.failed")
                        .increment();
                log.error(
                        "COMPENSATION FAILED for transaction {}. Reason: {}",
                        transactionId,
                        e.getMessage()
                );
            }

        } else {
            meterRegistry
                    .counter("banking.compensation.ignored")
                    .increment();
            log.warn(
                    "Ignoring credit failure for transaction {} because it is no longer CREDIT_PENDING",
                    transactionId
            );
        }
    }

    @Transactional
    @KafkaListener(
            topics = "transaction.credit.succeeded",
            groupId = "transaction-service-group"
    )
    public void consumeCreditSucceeded(@Payload Map<String, Object> payload) {
        String transactionId = payload.get("transactionId").toString();
        String eventId = "credit-succeeded-" + transactionId;

        int inserted = processedEventRepository.insertIfAbsent(
                eventId,
                "TRANSACTION_CREDIT_SUCCEEDED"
        );

        if (inserted == 0) {

            log.warn(
                    "Duplicate credit success event ignored for transaction {}",
                    transactionId
            );

            return;
        }

        int updated = transactionRepository.updateStatusIfCurrent(
                transactionId,
                TransactionStatus.CREDIT_PENDING,
                TransactionStatus.COMPLETED
        );

        if (updated == 1) {

            log.info(
                    "Transaction {} completed after receiver credit succeeded",
                    transactionId
            );

        } else {

            log.warn(
                    "Ignoring credit success for transaction {} because it is no longer CREDIT_PENDING",
                    transactionId
            );
        }
    }
}
