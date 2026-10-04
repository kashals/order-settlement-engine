package com.engine.order.publisher;

import com.engine.order.domain.OutboxStatus;
import com.engine.order.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Component
public class OutboxPurgeJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxPurgeJob.class);

    private final OutboxRepository outboxRepository;
    private final TransactionTemplate transactionTemplate;
    private final int retentionDays;
    private final int chunkSize;

    public OutboxPurgeJob(
        OutboxRepository outboxRepository,
        TransactionTemplate transactionTemplate,
        @Value("${outbox.purge.retention-days:7}") int retentionDays,
        @Value("${outbox.purge.chunk-size:1000}") int chunkSize
    ) {
        this.outboxRepository = outboxRepository;
        this.transactionTemplate = transactionTemplate;
        this.retentionDays = retentionDays;
        this.chunkSize = chunkSize;
    }

    // outbox purge
    @Scheduled(cron = "${outbox.purge.cron:0 0 2 * * *}")
    public void purgeProcessedEvents() {
        Instant cutoff = Instant.now().minus(retentionDays, ChronoUnit.DAYS);
        int totalDeleted = 0;
        int deleted;

        do {
            Integer count = transactionTemplate.execute(status ->
                outboxRepository.deleteChunkByStatusAndCreatedAtBefore(
                    OutboxStatus.SENT.name(),
                    cutoff,
                    chunkSize
                )
            );
            deleted = (count != null) ? count : 0;
            totalDeleted += deleted;
        } while (deleted == chunkSize);

        if (totalDeleted > 0) {
            log.info("Purged {} published outbox events older than {} days in chunks of {}", totalDeleted, retentionDays, chunkSize);
        }
    }
}
