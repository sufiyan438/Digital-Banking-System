package com.banking.transactionservice.repository;

import com.banking.transactionservice.model.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
    @Modifying
    @Query(
            value = """
                INSERT IGNORE INTO processed_events
                (event_id, event_type, processed_at)
                VALUES (:eventId, :eventType, NOW())
                """,
            nativeQuery = true
    )
    int insertIfAbsent(
            @Param("eventId") String eventId,
            @Param("eventType") String eventType
    );
}