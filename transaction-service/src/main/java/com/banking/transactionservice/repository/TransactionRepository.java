package com.banking.transactionservice.repository;

import com.banking.transactionservice.model.Transaction;
import com.banking.transactionservice.model.TransactionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, String> {
    List<Transaction> findAllBySenderAccountNumberOrderByCreatedAtDesc(String accountNumber);

    @Modifying
    @Transactional
    @Query("""
    UPDATE Transaction t
    SET t.status = :newStatus
    WHERE t.id = :transactionId
      AND t.status = :expectedStatus
""")
    int updateStatusIfCurrent(
            @Param("transactionId") String transactionId,
            @Param("expectedStatus") TransactionStatus expectedStatus,
            @Param("newStatus") TransactionStatus newStatus
    );

    List<Transaction> findAllByStatus(TransactionStatus status);
}
