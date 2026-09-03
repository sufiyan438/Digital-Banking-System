package com.banking.accountservice.repository;

import com.banking.accountservice.model.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

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