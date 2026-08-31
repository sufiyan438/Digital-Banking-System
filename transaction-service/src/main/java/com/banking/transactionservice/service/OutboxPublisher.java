package com.banking.transactionservice.service;

import com.banking.transactionservice.model.OutboxEvent;
import com.banking.transactionservice.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 5000)
    public void publishPendingEvents() {

        List<OutboxEvent> events =
                outboxEventRepository.findByStatus("PENDING");

        for (OutboxEvent event : events) {

            try {

                Map<String, Object> payload =
                        objectMapper.readValue(
                                event.getPayload(),
                                Map.class
                        );

                kafkaTemplate.send(
                        event.getTopic(),
                        event.getAggregateId(),
                        payload
                ).get();

                event.setStatus("PUBLISHED");
                event.setPublishedAt(LocalDateTime.now());

                outboxEventRepository.save(event);

                log.info(
                        "Outbox event {} published to topic {}",
                        event.getId(),
                        event.getTopic()
                );

            } catch (Exception e) {

                log.error(
                        "Failed to publish outbox event {}. Reason: {}",
                        event.getId(),
                        e.getMessage()
                );
            }
        }
    }
}