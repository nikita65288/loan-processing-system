package com.lps.decision.kafka;

import com.lps.contracts.event.DecisionCompletedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** A failed Kafka send must propagate to the listener rather than allow an offset commit. */
class DecisionProducerTest {
    @Test
    @SuppressWarnings("unchecked")
    void publicationFailurePropagates() {
        KafkaTemplate<String, DecisionCompletedEvent> template = mock(KafkaTemplate.class);
        DecisionCompletedEvent event = new com.lps.contracts.event.DecisionCompletedEvent(UUID.randomUUID(), "DecisionCompleted", Instant.now(), UUID.randomUUID(), UUID.randomUUID(), com.lps.contracts.model.Decision.APPROVED, "PASS");
        when(template.send(anyString(), anyString(), any(DecisionCompletedEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        assertThrows(IllegalStateException.class, () -> new DecisionProducer(template).publish(event));
    }
}
