package com.lps.bureau.kafka;

import com.lps.contracts.event.BureauCheckCompletedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** A failed Kafka send must propagate to the listener rather than allow an offset commit. */
class BureauProducerTest {
    @Test
    @SuppressWarnings("unchecked")
    void publicationFailurePropagates() {
        KafkaTemplate<String, BureauCheckCompletedEvent> template = mock(KafkaTemplate.class);
        BureauCheckCompletedEvent event = new com.lps.contracts.event.BureauCheckCompletedEvent(UUID.randomUUID(), "BureauCheckCompleted", Instant.now(), UUID.randomUUID(), UUID.randomUUID(), 720, false, true);
        when(template.send(anyString(), anyString(), any(BureauCheckCompletedEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        assertThrows(IllegalStateException.class, () -> new BureauProducer(template).publish(event));
    }
}
