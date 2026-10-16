package com.lps.creditscoring.kafka;

import com.lps.contracts.event.CreditScoringCompletedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** A failed Kafka send must propagate to the listener rather than allow an offset commit. */
class CreditScoringProducerTest {
    @Test
    @SuppressWarnings("unchecked")
    void publicationFailurePropagates() {
        KafkaTemplate<String, CreditScoringCompletedEvent> template = mock(KafkaTemplate.class);
        CreditScoringCompletedEvent event = new com.lps.contracts.event.CreditScoringCompletedEvent(UUID.randomUUID(), "CreditScoringCompleted", Instant.now(), UUID.randomUUID(), UUID.randomUUID(), 750, true);
        when(template.send(anyString(), anyString(), any(CreditScoringCompletedEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        assertThrows(IllegalStateException.class, () -> new CreditScoringProducer(template).publish(event));
    }
}
