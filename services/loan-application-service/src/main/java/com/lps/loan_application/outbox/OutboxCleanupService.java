package com.lps.loan_application.outbox;

import com.lps.loan_application.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Removes only old successfully published Outbox records.
 *
 * Keeping a retention window gives developers time to inspect publication
 * history while preventing the Outbox table from growing indefinitely.
 */
@Component
public class OutboxCleanupService {

    private static final Logger log = LoggerFactory.getLogger(OutboxCleanupService.class);

    private final OutboxEventRepository outboxEventRepository;
    private final boolean enabled;
    private final long retentionDays;
    private final TransactionTemplate transactionTemplate;

    public OutboxCleanupService(
            OutboxEventRepository outboxEventRepository,
            @Value("${lps.outbox.cleanup.enabled:true}") boolean enabled,
            @Value("${lps.outbox.cleanup.retention-days:7}") long retentionDays,
            org.springframework.transaction.PlatformTransactionManager transactionManager
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.enabled = enabled;
        this.retentionDays = retentionDays;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${lps.outbox.cleanup.interval-ms:3600000}")
    public void cleanupPublishedEvents() {
        if (!enabled) {
            return;
        }

        Instant threshold = Instant.now().minus(retentionDays, ChronoUnit.DAYS);

        Integer deleted = transactionTemplate.execute(status ->
                outboxEventRepository.deletePublishedBefore(
                        OutboxEventStatus.PUBLISHED,
                        threshold
                )
        );

        if (deleted != null && deleted > 0) {
            log.info(
                    "Outbox cleanup removed published events count={}, retentionDays={}",
                    deleted, retentionDays
            );
        }
    }
}
