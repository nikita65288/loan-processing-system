package com.lps.creditscoring.kafka;

import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.creditscoring.config.KafkaConfiguration;
import com.lps.creditscoring.service.CreditScoringStub;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.lps.contracts.event.CreditScoringFailedEvent;
import com.lps.contracts.MessageIdentity;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Consumes scoring commands and publishes the deterministic stub result. */
@Component
public class CreditScoringConsumer {
    private static final Logger log = LoggerFactory.getLogger(CreditScoringConsumer.class);
    private final CreditScoringStub stub;
    private final CreditScoringProducer producer;
    private final boolean failOnEvent;
    private final FailurePublisher failurePublisher;
    private final Environment environment;

    public CreditScoringConsumer(
            CreditScoringStub stub,
            CreditScoringProducer producer,
            @Value("${lps.kafka.consumer.fail-on-event:false}") boolean failOnEvent,
                          FailurePublisher failurePublisher, Environment environment
    ) {
        this.stub = stub;
        this.producer = producer;
        this.failOnEvent = failOnEvent;
        this.failurePublisher = failurePublisher;
        this.environment = environment;
    }

    @KafkaListener(
            topics = KafkaConfiguration.SCORING_REQUESTED_TOPIC,
            groupId = KafkaConfiguration.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, PerformCreditScoringCommand> record) {
        PerformCreditScoringCommand command = record.value();
        log.info(
                "Received PerformCreditScoringCommand commandId={}, applicationId={}, correlationId={}, key={}, partition={}, offset={}",
                command.commandId(),
                command.applicationId(),
                command.correlationId(),
                record.key(),
                record.partition(),
                record.offset()
        );
        if (failOnEvent) throw new IllegalStateException("Intentional Kafka consumer failure");
        // Fault injection is opt-in and isolated to the failure-simulation Spring profile.
        if (environment.acceptsProfiles(Profiles.of("failure-simulation"))) {
            String mode = environment.getProperty("lps.stub.failure-mode", "NORMAL");
            if ("RETRYABLE_FAILURE".equals(mode) || "PERMANENT_FAILURE".equals(mode)) {
                boolean retryable = "RETRYABLE_FAILURE".equals(mode);
                failurePublisher.publish(new CreditScoringFailedEvent(
                        MessageIdentity.resultEventId(command.commandId(), "CreditScoringFailed"),
                        "CreditScoringFailed", Instant.now(), command.correlationId(),
                        command.applicationId(), command.commandId(),
                        retryable ? "SCORING_TEMPORARILY_UNAVAILABLE" : "SCORING_INVALID_REQUEST",
                        retryable));
                return;
            }
            if (!"NORMAL".equals(mode)) {
                throw new IllegalArgumentException("Unknown failure simulation mode: " + mode);
            }
        }
        CreditScoringCompletedEvent event = stub.score(command);
        producer.publish(event);
    }
}
