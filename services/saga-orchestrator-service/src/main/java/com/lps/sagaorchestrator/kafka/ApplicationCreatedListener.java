package com.lps.sagaorchestrator.kafka;

import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Adapts Kafka delivery to the transactional initialization business service. */
@Component
public class ApplicationCreatedListener {
    private static final Logger log = LoggerFactory.getLogger(ApplicationCreatedListener.class);
    private final SagaOrchestrationService service;

    public ApplicationCreatedListener(SagaOrchestrationService service) {
        this.service = service;
    }

    @KafkaListener(
            topics = SagaKafkaConfiguration.APPLICATION_CREATED_TOPIC,
            groupId = SagaKafkaConfiguration.CONSUMER_GROUP,
            containerFactory = "sagaKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, ApplicationCreatedEvent> record) {
        ApplicationCreatedEvent event = record.value();
        service.initialize(event);
        log.info("Saga initialization processed applicationId={}, partition={}, offset={}",
                event.applicationId(), record.partition(), record.offset());
    }
}
