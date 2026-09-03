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


    /*

    this is a STATE GUARD i.e. update the row only if the row status is in the desired status
    Example:
    updateStatusIfCurrent(
    "TX123",
    TransactionStatus.PENDING_VERIFICATION,
    TransactionStatus.CREDIT_PENDING
);

Why is this being used instead of doing a normal if check?

    Transaction tx = repo.findById(...);

    if (tx.getStatus() == PENDING_VERIFICATION) {
        tx.setStatus(CREDIT_PENDING);
        repo.save(tx);
    }


BCOZ two concurrent requests can reach the same if condition cand cause a race condition.

Correct OTP path wants:                 Expiry scheduler wants:

PENDING_VERIFICATION                       PENDING_VERIFICATION
        ↓                                           ↓
CREDIT_PENDING                                 COMPENSATING


Now this will return a 1 if transition is in desired state, 0 if transaction was not in desired state.

@Modifying is because this is not a SELECT query but an UPDATE query
@Transactional - update happens inside database adn this is JPQL. Ensures commit happens correctly.
     */
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
