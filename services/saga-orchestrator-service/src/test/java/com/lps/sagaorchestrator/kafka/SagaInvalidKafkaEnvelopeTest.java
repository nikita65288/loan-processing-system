package com.lps.sagaorchestrator.kafka;

import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.contracts.event.DisbursementStatusInquiryResultEvent;
import com.lps.kafka.reliability.InvalidKafkaRecordException;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** Invalid Kafka envelopes must fail before the durable Saga Inbox is touched. */
class SagaInvalidKafkaEnvelopeTest {
    @Test
    void wrongEventTypeIsNotProcessed() {
        var orchestrator = mock(SagaOrchestrationService.class);
        var applicationId = UUID.randomUUID();
        var wrong = new CreditScoringCompletedEvent(UUID.randomUUID(), "NotCreditScoringCompleted",
                Instant.now(), UUID.randomUUID(), applicationId, 720, true);
        assertThrows(InvalidKafkaRecordException.class, () ->
                new CreditScoringCompletedListener(orchestrator).consume(
                        new ConsumerRecord<>(SagaKafkaConfiguration.SCORING_COMPLETED_TOPIC, 0, 0L,
                                applicationId.toString(), wrong)));
        verifyNoInteractions(orchestrator);
    }

    @Test
    void missingInquiryOutcomeIsNotAppliedAsFinancialSuccess() {
        var orchestrator = mock(SagaOrchestrationService.class);
        var applicationId = UUID.randomUUID();
        var invalid = new DisbursementStatusInquiryResultEvent(UUID.randomUUID(),
                "DisbursementStatusInquiryResult", Instant.now(), UUID.randomUUID(),
                applicationId, UUID.randomUUID(), UUID.randomUUID(), null, null, null, null);
        assertThrows(InvalidKafkaRecordException.class, () ->
                new DisbursementStatusInquiryResultListener(orchestrator).consume(
                        new ConsumerRecord<>(SagaKafkaConfiguration.DISBURSEMENT_INQUIRY_RESULT_TOPIC, 0, 0L,
                                applicationId.toString(), invalid)));
        verifyNoInteractions(orchestrator);
    }
}
