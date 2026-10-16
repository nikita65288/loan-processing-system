package com.lps.sagaorchestrator.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

/** Bounded startup visibility only; the existing publisher and listeners perform recovery. */
@Component
@ConditionalOnProperty(name = "lps.recovery.startup-scan-enabled", havingValue = "true", matchIfMissing = true)
public class SagaRecoveryStartupListener {
    private static final Logger log = LoggerFactory.getLogger(SagaRecoveryStartupListener.class);
    private final SagaRecoveryService inspection;

    public SagaRecoveryStartupListener(SagaRecoveryService inspection) {
        this.inspection = inspection;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void inspectAfterStartup() {
        log.info("Saga startup recovery inspection started");
        try {
            SagaRecoverySummary result = inspection.inspect();
            log.info("Saga startup recovery inspection completed inspected={} truncated={} classifications={}",
                    result.inspected(), result.truncated(), result.counts());
            if (result.truncated()) {
                log.warn("Saga inspection reached max-sagas limit: counts are incomplete");
            }
        } catch (DataAccessException exception) {
            // Startup inspection must not control normal message processing or create empty fallback state.
            log.error("Saga startup recovery inspection could not read PostgreSQL; no recovery action taken", exception);
        }
    }
}
