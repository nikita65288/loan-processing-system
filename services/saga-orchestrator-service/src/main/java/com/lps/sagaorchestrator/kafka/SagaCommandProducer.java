package com.lps.sagaorchestrator.kafka;

import com.lps.contracts.command.DisburseLoanCommand;
import com.lps.contracts.command.SendLoanDecisionNotificationCommand;
import com.lps.contracts.command.MakeDecisionCommand;
import com.lps.contracts.command.PerformBureauCheckCommand;
import com.lps.contracts.command.PerformCreditScoringCommand;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Sends all supported Saga command payloads; retries are coordinated by the existing Outbox. */
@Component
public class SagaCommandProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public SagaCommandProducer(@Qualifier("sagaKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, Object>> publish(String topic, Object command) {
        UUID applicationId;
        if (command instanceof PerformCreditScoringCommand c) applicationId = c.applicationId();
        else if (command instanceof PerformBureauCheckCommand c) applicationId = c.applicationId();
        else if (command instanceof MakeDecisionCommand c) applicationId = c.applicationId();
        else if (command instanceof DisburseLoanCommand c) applicationId = c.applicationId();
        else if (command instanceof SendLoanDecisionNotificationCommand c) applicationId = c.applicationId();
        else throw new IllegalArgumentException("Unsupported Saga command: " + command.getClass());
        return kafkaTemplate.send(topic, applicationId.toString(), command);
    }
}
