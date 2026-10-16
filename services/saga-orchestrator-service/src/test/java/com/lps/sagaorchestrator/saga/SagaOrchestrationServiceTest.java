package com.lps.sagaorchestrator.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.sagaorchestrator.cache.SagaCacheInvalidation;
import com.lps.sagaorchestrator.inbox.ProcessedMessageDao;
import com.lps.sagaorchestrator.outbox.SagaOutboxEvent;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.retry.SagaStepAttemptRepository;
import com.lps.sagaorchestrator.retry.SagaRetryProperties;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** Fast unit tests for contract mapping and the no-op duplicate path. */
class SagaOrchestrationServiceTest {
    private final SagaRepository sagas = mock(SagaRepository.class);
    private final SagaOutboxRepository outbox = mock(SagaOutboxRepository.class);
    private final ProcessedMessageDao inbox = mock(ProcessedMessageDao.class);
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final SagaStepAttemptRepository attempts = mock(SagaStepAttemptRepository.class);
    private final SagaRetryProperties policy = new SagaRetryProperties(
            new SagaRetryProperties.Timeout(true, Duration.ofSeconds(30), Duration.ofSeconds(45), Duration.ofSeconds(30)),
            new SagaRetryProperties.Retry(3, Duration.ofSeconds(5)),
            new SagaRetryProperties.Scheduler(50));
    private final SagaOrchestrationService service = new SagaOrchestrationService(
            sagas, outbox, inbox, mapper, attempts, policy, Clock.systemUTC(), new SagaCompensationPolicy(), mock(SagaCacheInvalidation.class),
            mock(com.lps.sagaorchestrator.observability.SagaMetrics.class));

    @Test
    void createsScoringCommandWithEventData() throws Exception {
        ApplicationCreatedEvent event = event();
        when(inbox.register(event.eventId(), "ApplicationCreated")).thenReturn(true);
        when(sagas.saveAndFlush(any(SagaInstance.class))).thenAnswer(invocation -> invocation.getArgument(0));
        service.initialize(event);
        ArgumentCaptor<SagaInstance> sagaCaptor = ArgumentCaptor.forClass(SagaInstance.class);
        verify(sagas).saveAndFlush(sagaCaptor.capture());
        assertEquals(event.applicationId(), sagaCaptor.getValue().getApplicationId());
        assertEquals(SagaStep.SCORING, sagaCaptor.getValue().getCurrentStep());
        ArgumentCaptor<SagaOutboxEvent> outboxCaptor = ArgumentCaptor.forClass(SagaOutboxEvent.class);
        verify(outbox).save(outboxCaptor.capture());
        var row = outboxCaptor.getValue();
        PerformCreditScoringCommand command = mapper.readValue(row.getPayload(), PerformCreditScoringCommand.class);
        assertEquals(row.getId(), command.commandId());
        assertEquals(event.customerId(), command.customerId());
        assertEquals(event.correlationId(), command.correlationId());
        assertEquals(event.applicationId(), command.applicationId());
        assertEquals(event.amount(), command.amount());
        verify(attempts).save(any());
    }

    @Test
    void replaySkipsBothWrites() {
        ApplicationCreatedEvent event = event();
        when(inbox.register(event.eventId(), "ApplicationCreated")).thenReturn(false);
        service.initialize(event);
        verify(sagas, never()).saveAndFlush(any());
        verify(outbox, never()).save(any());
    }

    private ApplicationCreatedEvent event() {
        return new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated",
                Instant.now(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("4500.00"), 9, "EUR");
    }
}
