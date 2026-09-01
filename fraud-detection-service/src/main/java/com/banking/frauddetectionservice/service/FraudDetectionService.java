package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {

    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;

    @Value("${fraud.max-transactions-per-minute}")
    private int maxTransactionsPerMinute;

    @Value("${fraud.suspicious-amount-multiplier}")
    private double  suspiciousAmountMultiplier;

    private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";
    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";

    public void checkTransaction(Map<String, Object> payload) throws Exception{
        String transactionId = payload.get("transactionId").toString();

        if (isAlreadyProcessed(transactionId)) {
            log.warn(
                    "Duplicate transaction.initiated event ignored for transaction {}",
                    transactionId
            );
            return;
        }

        String accountNumber = payload.get("senderAccountNumber").toString();
        BigDecimal amount = new BigDecimal(payload.get("amount").toString());

        BigDecimal senderBalance = accountServiceClient.getBalance(accountNumber);
        log.info("Checking transaction: {}, account: {}, amount: {}, balance: {}",
                transactionId, accountNumber, amount, senderBalance);

        FraudCheckResult result = performFraudChecks(accountNumber, amount, senderBalance);

        if(result.isFraud()){
            log.info("Suspicious activity detected in account {}. Reason: {}. Requesting OTP verification",
                    accountNumber, result.getReason());

            Map<String, Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("accountNumber", accountNumber);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", result.getReason());

//            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);
            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent).get();
            markAsProcessed(transactionId);
        }
        else{
            log.info("Transaction is clean!");
            Map<String, Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("isFraud", false);
            transactionCleanEvent.put("reason", null);

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, transactionCleanEvent).get();
            markAsProcessed(transactionId);
        }
    }

    private FraudCheckResult performFraudChecks(String accountNumber, BigDecimal amount, BigDecimal senderBalance){
        if(isVelocityExceeded(accountNumber)){
            return new FraudCheckResult(true, "Too many transactions in 60 sec. Velocity limit exceeded.");
        }
        if(isAmountSuspicious(accountNumber, amount)){
            return new FraudCheckResult(true, "Unusual transaction amount. It is 3x your average");
        }
        if(senderBalance.compareTo(BigDecimal.ZERO) > 0 && isBalanceCheckFailed(senderBalance, amount)){
            return new FraudCheckResult(true, "Transaction exceeds 90% of account balance");
        }
        return new FraudCheckResult(false, null);
    }

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount){
        String avgKey = "fraud:avg_amount" + accountNumber;
        String avgStr = redisTemplate.opsForValue().get(avgKey);

        if(avgStr == null){
            redisTemplate.opsForValue().set(avgKey, amount.toString());
            return false;
        }

        BigDecimal avgAmount = new BigDecimal(avgStr);
        BigDecimal threshold = avgAmount.multiply(BigDecimal.valueOf(suspiciousAmountMultiplier));

        BigDecimal newAvg = avgAmount.add(amount)
                .divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);
        redisTemplate.opsForValue().set(avgKey, newAvg.toString());
        log.info("Amount check - amount: {}, threshold: {}, suspicious: {}", amount, threshold,
                amount.compareTo(threshold) > 0);

        return amount.compareTo(threshold) > 0;
    }

    private boolean isVelocityExceeded(String accountNumber){
        String key = "fraud:velocity" + accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);
        if(count != null && count == 1){
            redisTemplate.expire(key, 60, TimeUnit.SECONDS);
        }
        log.info("Velocity check - account: {} count: {}/{}",
                accountNumber, count, maxTransactionsPerMinute);
        return count != null && count > maxTransactionsPerMinute;
    }

    private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount){
        BigDecimal maxAllowed = senderBalance.multiply(BigDecimal.valueOf(maxBalancePercentage));
        log.info("Balance check - amount: {}, maxAllowed: {}, suspicious: {}",
                amount, maxAllowed, amount.compareTo(maxAllowed) > 0);
        return amount.compareTo(maxAllowed) > 0;
    }

    private boolean isAlreadyProcessed(String transactionId) {
        String key = "fraud:processed:" + transactionId;
        return Boolean.TRUE.equals(
                redisTemplate.hasKey(key)
        );
//        Boolean firstTime = redisTemplate.opsForValue()
//                .setIfAbsent(
//                        key,
//                        "processed",
//                        1,
//                        TimeUnit.DAYS
//                );
//
//        return Boolean.FALSE.equals(firstTime);
    }

    private void markAsProcessed(String transactionId) {
        String key = "fraud:processed:" + transactionId;

        redisTemplate.opsForValue().set(
                key,
                "processed",
                1,
                TimeUnit.DAYS
        );
    }
}
