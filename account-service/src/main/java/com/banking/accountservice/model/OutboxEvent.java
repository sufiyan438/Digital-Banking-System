package com.banking.accountservice.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    private UUID id;

    private String aggregateId;

    private String eventType;

    private String topic;

    @Column(columnDefinition = "TEXT")
    private String payload;

    private String status;

    private LocalDateTime createdAt;

    private LocalDateTime publishedAt;
}