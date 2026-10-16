package com.lps.decision.kafka;

import com.lps.contracts.command.MakeDecisionCommand;
import com.lps.decision.config.KafkaConfiguration;
import com.lps.decision.service.DecisionStub;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.lps.contracts.event.DecisionFailedEvent;
import com.lps.contracts.MessageIdentity;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Consumes decision commands and publishes the deterministic decision result. */
@Component
public class DecisionConsumer {
    private static final Logger log = LoggerFactory.getLogger(DecisionConsumer.class);
    private final DecisionStub stub;
    private final DecisionProducer producer;
    private final boolean failOnEvent;
    private final FailurePublisher failurePublisher;
    private final Environment environment;

    public DecisionConsumer(DecisionStub stub, DecisionProducer producer,
                            @Value("${lps.kafka.consumer.fail-on-event:false}") boolean failOnEvent,
                          FailurePublisher failurePublisher, Environment environment) {
        this.stub = stub; this.producer = producer; this.failOnEvent = failOnEvent;
        this.failurePublisher = failurePublisher;
        this.environment = environment;
    }

    @KafkaListener(
            topics = KafkaConfiguration.DECISION_REQUESTED_TOPIC,
            groupId = KafkaConfiguration.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, MakeDecisionCommand> record) {
        MakeDecisionCommand command = record.value();
        log.info(
                "Received MakeDecisionCommand commandId={}, applicationId={}, correlationId={}, key={}, partition={}, offset={}",
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
                failurePublisher.publish(new DecisionFailedEvent(
                        MessageIdentity.resultEventId(command.commandId(), "DecisionFailed"),
                        "DecisionFailed", Instant.now(), command.correlationId(),
                        command.applicationId(), command.commandId(),
                        retryable ? "DECISION_TEMPORARILY_UNAVAILABLE" : "DECISION_INVALID_REQUEST",
                        retryable));
                return;
            }
            if (!"NORMAL".equals(mode)) {
                throw new IllegalArgumentException("Unknown failure simulation mode: " + mode);
            }
        }
        producer.publish(stub.decide(command));
    }
}
