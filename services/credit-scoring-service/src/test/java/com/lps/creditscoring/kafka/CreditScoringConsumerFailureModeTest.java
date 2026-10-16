package com.lps.creditscoring.kafka;

import com.lps.creditscoring.service.CreditScoringStub;
import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.CreditScoringFailedEvent;
import com.lps.contracts.MessageIdentity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

import java.time.Instant;
import java.util.UUID;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Fault injection requires an explicit Spring profile and emits a typed technical event. */
class CreditScoringConsumerFailureModeTest {
    @Test
    void permanentFailurePublishesStableEventWithoutCompletedResult() {
        var command = new PerformCreditScoringCommand(UUID.randomUUID(), "PerformCreditScoring", Instant.now(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("5000"), 12, "EUR");
        var stub = mock(CreditScoringStub.class);
        var completed = mock(CreditScoringProducer.class);
        var failures = mock(FailurePublisher.class);
        var environment = mock(Environment.class);
        when(environment.acceptsProfiles(any(Profiles.class))).thenReturn(true);
        when(environment.getProperty("lps.stub.failure-mode", "NORMAL")).thenReturn("PERMANENT_FAILURE");
        new CreditScoringConsumer(stub, completed, false, failures, environment).consume(
                new ConsumerRecord<>("test", 0, 0L, command.applicationId().toString(), command));
        var captured = ArgumentCaptor.forClass(CreditScoringFailedEvent.class);
        verify(failures).publish(captured.capture());
        var failure = captured.getValue();
        assertEquals(command.commandId(), failure.commandId());
        assertEquals(command.applicationId(), failure.applicationId());
        assertEquals(MessageIdentity.resultEventId(command.commandId(), "CreditScoringFailed"), failure.eventId());
        assertFalse(failure.retryable());
        verifyNoInteractions(completed, stub);
    }

    @Test
    void normalProfileIgnoresFailureInjectionConfiguration() {
        var command = new PerformCreditScoringCommand(UUID.randomUUID(), "PerformCreditScoring", Instant.now(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("5000"), 12, "EUR");
        var stub = mock(CreditScoringStub.class);
        var completed = mock(CreditScoringProducer.class);
        var failures = mock(FailurePublisher.class);
        var environment = mock(Environment.class);
        when(environment.acceptsProfiles(any(Profiles.class))).thenReturn(false);
        new CreditScoringConsumer(stub, completed, false, failures, environment).consume(
                new ConsumerRecord<>("test", 0, 0L, command.applicationId().toString(), command));
        verifyNoInteractions(failures);
        verify(stub).score(command);
    }
}
