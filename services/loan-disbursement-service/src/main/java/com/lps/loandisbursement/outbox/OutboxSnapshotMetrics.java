package com.lps.loandisbursement.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Periodic DB snapshot; Prometheus reads atomic values without querying PostgreSQL. */
@Component
public class OutboxSnapshotMetrics {
    private static final Logger log = LoggerFactory.getLogger(OutboxSnapshotMetrics.class);
    private static final String TABLE = "loan_disbursement.outbox_events";
    private final JdbcTemplate jdbc;
    private final Map<String, AtomicLong> counts = Map.of(
            "NEW", new AtomicLong(-1), "CLAIMED", new AtomicLong(-1), "FAILED", new AtomicLong(-1));
    private final AtomicLong oldestSeconds = new AtomicLong(-1);

    public OutboxSnapshotMetrics(JdbcTemplate jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;
        counts.forEach((status, count) -> Gauge.builder("lps.outbox.records", count, AtomicLong::get)
                .tag("status", status).register(registry));
        Gauge.builder("lps.outbox.oldest.pending.seconds", oldestSeconds, AtomicLong::get)
                .register(registry);
    }

    @Scheduled(initialDelay = 5000, fixedDelayString = "${lps.observability.snapshot-interval-ms:30000}")
    public void refresh() {
        try {
            Map<String, Long> values = new java.util.HashMap<>();
            jdbc.query("SELECT status, COUNT(*) AS total FROM " + TABLE +
                    " WHERE status IN ('NEW','CLAIMED','FAILED') GROUP BY status", rs -> {
                values.put(rs.getString("status"), rs.getLong("total"));
            });
            counts.forEach((status, count) -> count.set(values.getOrDefault(status, 0L)));
            Number age = jdbc.queryForObject("SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MIN(created_at))) " +
                    "FROM " + TABLE + " WHERE status IN ('NEW','CLAIMED')", Number.class);
            oldestSeconds.set(age == null ? 0 : Math.max(0, age.longValue()));
        } catch (DataAccessException ex) {
            counts.values().forEach(count -> count.set(-1));
            oldestSeconds.set(-1);
            log.warn("Outbox metric snapshot unavailable (value -1 indicates unknown): {}", ex.toString());
        }
    }
}