package com.lps.sagaorchestrator.kafka;

import com.lps.contracts.event.LoanDisbursementCompletedEvent;
import com.lps.contracts.event.LoanDisbursementFailedEvent;
import com.lps.contracts.event.LoanNotificationCompletedEvent;
import com.lps.contracts.event.LoanNotificationFailedEvent;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.contracts.event.BureauCheckCompletedEvent;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.contracts.event.DecisionCompletedEvent;
import com.lps.contracts.event.CreditScoringFailedEvent;
import com.lps.contracts.event.BureauCheckFailedEvent;
import com.lps.contracts.event.DecisionFailedEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/** One consumer group and explicit JSON value types per topic; no producer type headers required. */
@Configuration
public class SagaKafkaConfiguration {
    public static final String APPLICATION_CREATED_TOPIC = "loan.application.created";
    public static final String SCORING_REQUESTED_TOPIC = "loan.scoring.requested";
    public static final String SCORING_COMPLETED_TOPIC = "loan.scoring.completed";
    public static final String BUREAU_REQUESTED_TOPIC = "loan.bureau.check.requested";
    public static final String BUREAU_COMPLETED_TOPIC = "loan.bureau.check.completed";
    public static final String DECISION_REQUESTED_TOPIC = "loan.decision.requested";
    public static final String DECISION_COMPLETED_TOPIC = "loan.decision.completed";
    public static final String SCORING_FAILED_TOPIC = "loan.scoring.failed";
    public static final String BUREAU_FAILED_TOPIC = "loan.bureau.check.failed";
    public static final String DECISION_FAILED_TOPIC = "loan.decision.failed";
    public static final String DISBURSEMENT_REQUESTED_TOPIC = "loan.disbursement.requested";
    public static final String DISBURSEMENT_COMPLETED_TOPIC = "loan.disbursement.completed";
    public static final String DISBURSEMENT_FAILED_TOPIC = "loan.disbursement.failed";
    public static final String NOTIFICATION_REQUESTED_TOPIC = "loan.notification.requested";
    public static final String NOTIFICATION_COMPLETED_TOPIC = "loan.notification.completed";
    public static final String NOTIFICATION_FAILED_TOPIC = "loan.notification.failed";
    public static final String CONSUMER_GROUP = "saga-orchestrator-service";

    @Bean NewTopic applicationCreatedTopic() {return new NewTopic(APPLICATION_CREATED_TOPIC, 3, (short) 1);}
    @Bean NewTopic scoringRequestedTopic() {return new NewTopic(SCORING_REQUESTED_TOPIC, 3, (short) 1);}
    @Bean NewTopic scoringCompletedTopic() {return new NewTopic(SCORING_COMPLETED_TOPIC, 3, (short) 1);}
    @Bean NewTopic bureauRequestedTopic() {return new NewTopic(BUREAU_REQUESTED_TOPIC, 3, (short) 1);}
    @Bean NewTopic bureauCompletedTopic() {return new NewTopic(BUREAU_COMPLETED_TOPIC, 3, (short) 1);}
    @Bean NewTopic decisionRequestedTopic() {return new NewTopic(DECISION_REQUESTED_TOPIC, 3, (short) 1);}
    @Bean NewTopic decisionCompletedTopic() {return new NewTopic(DECISION_COMPLETED_TOPIC, 3, (short) 1);}

    @Bean NewTopic scoringFailedTopic() {return new NewTopic(SCORING_FAILED_TOPIC, 3, (short) 1);}
    @Bean NewTopic bureauFailedTopic() {return new NewTopic(BUREAU_FAILED_TOPIC, 3, (short) 1);}
    @Bean NewTopic decisionFailedTopic() {return new NewTopic(DECISION_FAILED_TOPIC, 3, (short) 1);}

    @Bean NewTopic disbursementRequestedTopic(){return new NewTopic(DISBURSEMENT_REQUESTED_TOPIC,3,(short)1);}
    @Bean NewTopic disbursementCompletedTopic(){return new NewTopic(DISBURSEMENT_COMPLETED_TOPIC,3,(short)1);}
    @Bean NewTopic disbursementFailedTopic(){return new NewTopic(DISBURSEMENT_FAILED_TOPIC,3,(short)1);}
    @Bean NewTopic notificationRequestedTopic(){return new NewTopic(NOTIFICATION_REQUESTED_TOPIC,3,(short)1);}
    @Bean NewTopic notificationCompletedTopic(){return new NewTopic(NOTIFICATION_COMPLETED_TOPIC,3,(short)1);}
    @Bean NewTopic notificationFailedTopic(){return new NewTopic(NOTIFICATION_FAILED_TOPIC,3,(short)1);}

    private <T> ConsumerFactory<String, T> eventConsumerFactory(String brokers, Class<T> type) {
        Map<String, Object> properties = new HashMap<>();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<T> deserializer = new JsonDeserializer<>(type, false);
        deserializer.addTrustedPackages("com.lps.contracts");

        return new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(), deserializer);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> eventListenerFactory(
            ConsumerFactory<String, T> consumerFactory,
            boolean enabled
    ) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, T>();

        factory.setConsumerFactory(consumerFactory);
        factory.setAutoStartup(enabled);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setRecordInterceptor(new com.lps.sagaorchestrator.observability.KafkaMdcInterceptor<>());
        // Never recover/skip a failed DB transaction by committing the Kafka offset.
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS)));

        return factory;
    }

    @Bean
    ConsumerFactory<String, ApplicationCreatedEvent> sagaConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers) {
        return eventConsumerFactory(brokers, ApplicationCreatedEvent.class);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, ApplicationCreatedEvent> sagaKafkaListenerContainerFactory(
            ConsumerFactory<String, ApplicationCreatedEvent> sagaConsumerFactory,
            @Value("${lps.kafka.consumer.enabled:true}") boolean enabled) {
        return eventListenerFactory(sagaConsumerFactory, enabled);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, CreditScoringCompletedEvent> scoringCompletedListenerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${lps.kafka.consumer.enabled:true}") boolean enabled
    ) {
        return eventListenerFactory(eventConsumerFactory(brokers, CreditScoringCompletedEvent.class), enabled);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, BureauCheckCompletedEvent> bureauCompletedListenerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${lps.kafka.consumer.enabled:true}") boolean enabled
    ) {
        return eventListenerFactory(eventConsumerFactory(brokers, BureauCheckCompletedEvent.class), enabled);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, DecisionCompletedEvent> decisionCompletedListenerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${lps.kafka.consumer.enabled:true}") boolean enabled
    ) {
        return eventListenerFactory(eventConsumerFactory(brokers, DecisionCompletedEvent.class), enabled);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, CreditScoringFailedEvent> scoringFailedListenerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${lps.kafka.consumer.enabled:true}") boolean enabled) {
        return eventListenerFactory(eventConsumerFactory(brokers, CreditScoringFailedEvent.class), enabled);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, BureauCheckFailedEvent> bureauFailedListenerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${lps.kafka.consumer.enabled:true}") boolean enabled) {
        return eventListenerFactory(eventConsumerFactory(brokers, BureauCheckFailedEvent.class), enabled);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, DecisionFailedEvent> decisionFailedListenerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${lps.kafka.consumer.enabled:true}") boolean enabled) {
        return eventListenerFactory(eventConsumerFactory(brokers, DecisionFailedEvent.class), enabled);
    }

    @Bean ConcurrentKafkaListenerContainerFactory<String,LoanDisbursementCompletedEvent> disbursementCompletedListenerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${lps.kafka.consumer.enabled:true}") boolean enabled) {
        return eventListenerFactory(eventConsumerFactory(brokers,LoanDisbursementCompletedEvent.class),enabled);
    }
    @Bean ConcurrentKafkaListenerContainerFactory<String,LoanDisbursementFailedEvent> disbursementFailedListenerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${lps.kafka.consumer.enabled:true}") boolean enabled) {
        return eventListenerFactory(eventConsumerFactory(brokers,LoanDisbursementFailedEvent.class),enabled);
    }
    @Bean ConcurrentKafkaListenerContainerFactory<String,LoanNotificationCompletedEvent> notificationCompletedListenerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${lps.kafka.consumer.enabled:true}") boolean enabled) {
        return eventListenerFactory(eventConsumerFactory(brokers,LoanNotificationCompletedEvent.class),enabled);
    }
    @Bean ConcurrentKafkaListenerContainerFactory<String,LoanNotificationFailedEvent> notificationFailedListenerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${lps.kafka.consumer.enabled:true}") boolean enabled) {
        return eventListenerFactory(eventConsumerFactory(brokers,LoanNotificationFailedEvent.class),enabled);
    }

    @Bean
    ProducerFactory<String, Object> sagaProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20_000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        properties.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    KafkaTemplate<String, Object> sagaKafkaTemplate(ProducerFactory<String, Object> sagaProducerFactory) {
        var template = new KafkaTemplate<>(sagaProducerFactory);
        template.setObservationEnabled(true);
        return template;
    }
}
