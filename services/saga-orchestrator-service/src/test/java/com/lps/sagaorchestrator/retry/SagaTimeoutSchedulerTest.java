package com.lps.sagaorchestrator.retry;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.*;

/** A poisoned first batch must not prevent later due Saga from being inspected. */
class SagaTimeoutSchedulerTest {
    @Test
    void rotatesDueBatchesAndContinuesAfterOneFailure() {
        var attempts = mock(SagaStepAttemptRepository.class);
        var retry = mock(SagaRetryService.class);
        var policy = new SagaRetryProperties(
                new SagaRetryProperties.Timeout(true, Duration.ofSeconds(30),
                        Duration.ofSeconds(45), Duration.ofSeconds(30)),
                new SagaRetryProperties.Retry(3, Duration.ofSeconds(5)),
                new SagaRetryProperties.Scheduler(2));
        Instant now = Instant.parse("2026-10-12T09:00:00Z");
        var scheduler = new SagaTimeoutScheduler(attempts, retry, policy,
                Clock.fixed(now, ZoneOffset.UTC));
        UUID a = UUID.randomUUID(), b = UUID.randomUUID(), c = UUID.randomUUID();
        when(attempts.findDueSagaIds(now, 3, 2, 0)).thenReturn(List.of(a, b));
        when(attempts.findDueSagaIds(now, 3, 2, 2)).thenReturn(List.of(c));
        doThrow(new IllegalStateException("poison")).when(retry).process(a);

        scheduler.checkTimeouts();
        scheduler.checkTimeouts();

        verify(retry).process(a);
        verify(retry).process(b);
        verify(retry).process(c);
        verify(attempts).findDueSagaIds(now, 3, 2, 0);
        verify(attempts).findDueSagaIds(now, 3, 2, 2);
    }
}
