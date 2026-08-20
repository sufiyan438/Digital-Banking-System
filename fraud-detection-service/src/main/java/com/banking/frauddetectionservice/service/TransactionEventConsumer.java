package com.banking.frauddetectionservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class TransactionEventConsumer {

    @Autowired
    private FraudDetectionService fraudDetectionService;

    @KafkaListener(topics = "transaction.initiated", groupId = "fraud-detection-group")
    public void consumeTransactionInitiate(@Payload Map<String, Object> payload){
        log.info("Received transaction {} for fraud check", payload.get("transactionId"));
        try{
            fraudDetectionService.checkTransaction(payload);
        } catch (Exception e) {
            log.error("Error in fraud detection: {}", e.getMessage());
        }
    }
}
