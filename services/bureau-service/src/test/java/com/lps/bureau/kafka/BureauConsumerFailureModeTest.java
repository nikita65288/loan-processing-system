package com.lps.bureau.kafka;

import com.lps.bureau.service.BureauStub;
import com.lps.contracts.command.PerformBureauCheckCommand;
import com.lps.contracts.event.BureauCheckFailedEvent;
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
class BureauConsumerFailureModeTest {
    @Test
    void permanentFailurePublishesStableEventWithoutCompletedResult() {
        var command = new PerformBureauCheckCommand(UUID.randomUUID(), "PerformBureauCheck", Instant.now(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        var stub = mock(BureauStub.class);
        var completed = mock(BureauProducer.class);
        var failures = mock(FailurePublisher.class);
        var environment = mock(Environment.class);
        when(environment.acceptsProfiles(any(Profiles.class))).thenReturn(true);
        when(environment.getProperty("lps.stub.failure-mode", "NORMAL")).thenReturn("PERMANENT_FAILURE");
        new BureauConsumer(stub, completed, false, failures, environment).consume(
                new ConsumerRecord<>("test", 0, 0L, command.applicationId().toString(), command));
        var captured = ArgumentCaptor.forClass(BureauCheckFailedEvent.class);
        verify(failures).publish(captured.capture());
        var failure = captured.getValue();
        assertEquals(command.commandId(), failure.commandId());
        assertEquals(command.applicationId(), failure.applicationId());
        assertEquals(MessageIdentity.resultEventId(command.commandId(), "BureauCheckFailed"), failure.eventId());
        assertFalse(failure.retryable());
        verifyNoInteractions(completed, stub);
    }

    @Test
    void normalProfileIgnoresFailureInjectionConfiguration() {
        var command = new PerformBureauCheckCommand(UUID.randomUUID(), "PerformBureauCheck", Instant.now(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        var stub = mock(BureauStub.class);
        var completed = mock(BureauProducer.class);
        var failures = mock(FailurePublisher.class);
        var environment = mock(Environment.class);
        when(environment.acceptsProfiles(any(Profiles.class))).thenReturn(false);
        new BureauConsumer(stub, completed, false, failures, environment).consume(
                new ConsumerRecord<>("test", 0, 0L, command.applicationId().toString(), command));
        verifyNoInteractions(failures);
        verify(stub).check(command);
    }
}
