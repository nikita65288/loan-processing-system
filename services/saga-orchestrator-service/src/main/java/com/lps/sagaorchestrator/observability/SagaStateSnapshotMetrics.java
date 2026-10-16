package com.lps.sagaorchestrator.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.concurrent.atomic.AtomicLong;

/** Cheap periodic durable-state gauges; never queries the database during a Prometheus scrape. */
@Component
public class SagaStateSnapshotMetrics {
    private static final Logger log = LoggerFactory.getLogger(SagaStateSnapshotMetrics.class);
    private final JdbcTemplate jdbc;
    private final AtomicLong active = new AtomicLong(-1);
    private final AtomicLong reconciliation = new AtomicLong(-1);
    public SagaStateSnapshotMetrics(JdbcTemplate jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;
        Gauge.builder("lps.saga.active", active, AtomicLong::get).register(registry);
        Gauge.builder("lps.saga.reconciliation.pending", reconciliation, AtomicLong::get).register(registry);
    }
    @Scheduled(initialDelay=5000, fixedDelayString="${lps.observability.snapshot-interval-ms:30000}")
    public void refresh() {
        try {
            Long count = jdbc.queryForObject("SELECT COUNT(*) FROM saga_orchestrator.saga_instances WHERE status='IN_PROGRESS'", Long.class);
            Long needsReview = jdbc.queryForObject("SELECT COUNT(*) FROM saga_orchestrator.saga_instances WHERE status='REQUIRES_RECONCILIATION'", Long.class);
            active.set(count == null ? 0 : count);
            reconciliation.set(needsReview == null ? 0 : needsReview);
        } catch (DataAccessException ex) {
            active.set(-1); reconciliation.set(-1);
            log.warn("Saga gauge snapshot unavailable: {}", ex.toString());
        }
    }
}
