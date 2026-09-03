package com.banking.accountservice.repository;

import com.banking.accountservice.model.ProcessedRefund;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedRefundRepository extends JpaRepository<ProcessedRefund, String> {
    @Modifying
    @Query(
            value = """
                    INSERT IGNORE INTO processed_refunds
                    (transaction_id, processed_at)
                    VALUES (:transactionId, NOW())
                    """,
            nativeQuery = true
    )
    int insertIfAbsent(@Param("transactionId") String transactionId);
}