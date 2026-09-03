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


/*
* this repo is for idempotent kafka consumption
* @Modifying tells SQL that this is not a SELECT query but a
* INSERT IGNORE - inserts the event only if that eventid is not already inserted
*
* query returns 1 if not present and then event is inserted
* query returns 0 if already present
*
*
* Why not do existsById()?
* if (!processedEventRepository.existsById(eventId)) {
    processedEventRepository.save(...);
    processEvent();
}
*
* BCOZ here a race condition can happen between two threads
*
* @Query - says this is not JPQL query but nativeQuery
*
*two reliability mechanisms complement each other:

OutboxPublisher: might send more than once rather than lose the event.

ProcessedEventRepository: makes sure I don't process the duplicate more than once.
*
*if native query is false, then it uses java entity names and if true use SQL entity name directly
* */