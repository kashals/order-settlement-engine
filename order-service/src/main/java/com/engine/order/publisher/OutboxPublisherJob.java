package com.engine.order.publisher;

import com.engine.order.domain.OutboxEvent;
import com.engine.order.domain.OutboxStatus;
import com.engine.order.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Component
public class OutboxPublisherJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherJob.class);
    private static final String TOPIC = "order.events";

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int batchSize;

    public OutboxPublisherJob(
        OutboxRepository outboxRepository,
        KafkaTemplate<String, String> kafkaTemplate,
        @Value("${outbox.polling.batch-size:50}") int batchSize
    ) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.batchSize = batchSize;
    }

    // outbox poller
    @Scheduled(fixedDelayString = "${outbox.polling.fixed-delay-ms:3000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxRepository.findByStatusOrderByCreatedAtAsc(
            OutboxStatus.PENDING,
            PageRequest.of(0, batchSize)
        );

        if (pendingEvents.isEmpty()) {
            return;
        }

        for (OutboxEvent event : pendingEvents) {
            try {
                kafkaTemplate.send(TOPIC, event.getAggregateId(), event.getPayload()).get();
                event.setStatus(OutboxStatus.SENT);
                event.setSentAt(Instant.now());
                outboxRepository.save(event);
            } catch (Exception e) {
                event.incrementRetryCount();
                if (event.getRetryCount() >= 5) {
                    event.setStatus(OutboxStatus.FAILED);
                }
                outboxRepository.save(event);
                log.error("Failed to publish outbox event id={}: {}", event.getId(), e.getMessage());
            }
        }
    }
}
