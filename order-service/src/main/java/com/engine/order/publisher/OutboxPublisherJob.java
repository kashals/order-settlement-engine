package com.engine.order.publisher;

import com.engine.order.domain.OutboxEvent;
import com.engine.order.domain.OutboxStatus;
import com.engine.order.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisherJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherJob.class);
    private static final String TOPIC = "order.events";

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${outbox.polling.batch-size:50}")
    private int batchSize;

    public OutboxPublisherJob(OutboxRepository outboxRepository, KafkaTemplate<String, Object> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    // outbox poller
    @Scheduled(fixedDelayString = "${outbox.polling.fixed-delay-ms:3000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxRepository.findPendingEventsWithLock(
                OutboxStatus.PENDING.name(),
                batchSize
        );

        if (pendingEvents.isEmpty()) {
            return;
        }

        for (OutboxEvent event : pendingEvents) {
            try {
                kafkaTemplate.send(TOPIC, event.getAggregateId(), event.getPayload())
                        .get(3, TimeUnit.SECONDS);

                event.setStatus(OutboxStatus.SENT);
                event.setSentAt(Instant.now());
                outboxRepository.save(event);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Outbox publisher interrupted for event id={}", event.getId());
                return;
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