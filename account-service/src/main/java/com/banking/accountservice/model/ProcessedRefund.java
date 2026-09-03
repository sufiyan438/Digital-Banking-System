package com.banking.accountservice.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "processed_refunds")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProcessedRefund {

    @Id
    private String transactionId;

    private LocalDateTime processedAt;
}

/*
this class is needed for business level idempotency
to check if this refund has already been processed
 */