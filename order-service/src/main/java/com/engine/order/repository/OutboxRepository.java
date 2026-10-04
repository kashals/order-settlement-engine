package com.engine.order.repository;

import com.engine.order.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    // outbox poller
    @Query(value = """
            SELECT * FROM outbox_events
            WHERE status = :status
            ORDER BY created_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> findPendingEventsWithLock(@Param("status") String status, @Param("limit") int limit);

    // outbox purge
    @org.springframework.data.jpa.repository.Modifying
    @Query(value = """
            DELETE FROM outbox_events
            WHERE id IN (
                SELECT id FROM outbox_events
                WHERE status = :status AND created_at < :cutoff
                LIMIT :limit
            )
            """, nativeQuery = true)
    int deleteChunkByStatusAndCreatedAtBefore(
        @Param("status") String status,
        @Param("cutoff") java.time.Instant cutoff,
        @Param("limit") int limit
    );
}
