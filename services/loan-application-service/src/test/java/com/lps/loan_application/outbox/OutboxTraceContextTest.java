package com.lps.loan_application.outbox;

import io.opentelemetry.context.Scope;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** W3C metadata can be restored in an unrelated publisher thread, including after restart. */
class OutboxTraceContextTest {
    private static final String PARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Test void restoresDurableW3cParent() {
        try (Scope ignored = OutboxTraceContext.resume(PARENT)) {
            assertEquals(PARENT, OutboxTraceContext.capture());
        }
    }

    @Test void legacyRowWithoutMetadataIsAllowed() {
        assertDoesNotThrow(() -> {
            try (Scope ignored = OutboxTraceContext.resume(null)) {
                // No requirement to synthesize a business traceId for old records.
            }
        });
    }
}
