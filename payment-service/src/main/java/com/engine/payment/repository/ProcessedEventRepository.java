package com.engine.payment.repository;

import com.engine.payment.domain.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {

    @Modifying
    @Query(value = "INSERT INTO processed_events (idempotency_key, consumer_name, processed_at) VALUES (:key, :consumerName, CURRENT_TIMESTAMP) ON CONFLICT (idempotency_key) DO NOTHING", nativeQuery = true)
    int insertIfNotExists(@Param("key") String key, @Param("consumerName") String consumerName);
}
