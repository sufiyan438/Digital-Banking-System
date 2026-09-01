package com.banking.accountservice.repository;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import com.banking.accountservice.model.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AccountRepository extends JpaRepository<Account, String> {
    boolean existsByAccountNumber(String accountNumber);
    boolean existsByEmail(String email);
    Optional<Account> findByAccountNumber(String accountNumber);

    @Modifying
    @Transactional
    @Query("""
    UPDATE Account a
    SET a.balance = a.balance - :amount
    WHERE a.accountNumber = :accountNumber
      AND a.balance >= :amount
""")
    int deductBalanceAtomic(
            @Param("accountNumber") String accountNumber,
            @Param("amount") BigDecimal amount
    );

    @Modifying
    @Transactional
    @Query("""
    UPDATE Account a
    SET a.balance = a.balance + :amount
    WHERE a.accountNumber = :accountNumber
""")
    int creditBalanceAtomic(
            @Param("accountNumber") String accountNumber,
            @Param("amount") BigDecimal amount
    );
}
