package com.banking.accountservice.repository;

import com.banking.accountservice.model.ProcessedRefund;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedRefundRepository extends JpaRepository<ProcessedRefund, String> {
}