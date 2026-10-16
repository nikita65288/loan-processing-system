package com.lps.bureau.kafka;

import com.lps.bureau.config.KafkaConfiguration;
import com.lps.bureau.service.BureauStub;
import com.lps.contracts.command.PerformBureauCheckCommand;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.lps.contracts.event.BureauCheckFailedEvent;
import com.lps.contracts.MessageIdentity;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Consumes bureau commands and publishes the deterministic stub result. */
@Component
public class BureauConsumer {
    private static final Logger log = LoggerFactory.getLogger(BureauConsumer.class);
    private final BureauStub stub;
    private final BureauProducer producer;
    private final boolean failOnEvent;
    private final FailurePublisher failurePublisher;
    private final Environment environment;

    public BureauConsumer(BureauStub stub, BureauProducer producer,
                          @Value("${lps.kafka.consumer.fail-on-event:false}") boolean failOnEvent,
                          FailurePublisher failurePublisher, Environment environment
    ) {
        this.stub = stub; this.producer = producer; this.failOnEvent = failOnEvent;
        this.failurePublisher = failurePublisher;
        this.environment = environment;
    }

    @KafkaListener(
            topics = KafkaConfiguration.BUREAU_REQUESTED_TOPIC,
            groupId = KafkaConfiguration.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, PerformBureauCheckCommand> record) {
        PerformBureauCheckCommand command = record.value();
        log.info(
                "Received PerformBureauCheckCommand commandId={}, applicationId={}, correlationId={}, key={}, partition={}, offset={}",
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
                failurePublisher.publish(new BureauCheckFailedEvent(
                        MessageIdentity.resultEventId(command.commandId(), "BureauCheckFailed"),
                        "BureauCheckFailed", Instant.now(), command.correlationId(),
                        command.applicationId(), command.commandId(),
                        retryable ? "BUREAU_TEMPORARILY_UNAVAILABLE" : "BUREAU_INVALID_REQUEST",
                        retryable));
                return;
            }
            if (!"NORMAL".equals(mode)) {
                throw new IllegalArgumentException("Unknown failure simulation mode: " + mode);
            }
        }
        producer.publish(stub.check(command));
    }
}
