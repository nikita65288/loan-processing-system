package com.lps.bureau.config;

import com.lps.contracts.command.PerformBureauCheckCommand;
import com.lps.contracts.event.BureauCheckCompletedEvent;
import com.lps.contracts.event.ProcessingFailureEvent;
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
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {
    public static final String BUREAU_REQUESTED_TOPIC = "loan.bureau.check.requested";
    public static final String BUREAU_COMPLETED_TOPIC = "loan.bureau.check.completed";
    public static final String BUREAU_FAILED_TOPIC = "loan.bureau.check.failed";
    public static final String CONSUMER_GROUP = "bureau-service";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    ProducerFactory<String, BureauCheckCompletedEvent> producerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    KafkaTemplate<String, BureauCheckCompletedEvent> kafkaTemplate(ProducerFactory<String, BureauCheckCompletedEvent> factory) {
        var template = new KafkaTemplate<>(factory);
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    ConsumerFactory<String, PerformBureauCheckCommand> consumerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        JsonDeserializer<PerformBureauCheckCommand> deserializer = new JsonDeserializer<>(PerformBureauCheckCommand.class);
        deserializer.addTrustedPackages("com.lps.contracts");
        return new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(), deserializer);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, PerformBureauCheckCommand> kafkaListenerContainerFactory(
            ConsumerFactory<String, PerformBureauCheckCommand> consumerFactory
    ) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, PerformBureauCheckCommand>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setRecordInterceptor(new com.lps.bureau.observability.KafkaMdcInterceptor<>());
        // Do not acknowledge a command if its required result event was not published.
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS)));
        return factory;
    }

    /** Explicit technical failures use a separate typed template; existing completed-event producer remains unchanged. */
    @Bean
    ProducerFactory<String, ProcessingFailureEvent> failureProducerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    KafkaTemplate<String, ProcessingFailureEvent> failureKafkaTemplate(
            ProducerFactory<String, ProcessingFailureEvent> failureProducerFactory) {
        var template = new KafkaTemplate<>(failureProducerFactory);
        template.setObservationEnabled(true);
        return template;
    }

    @Bean NewTopic bureauFailedTopic() { return new NewTopic(BUREAU_FAILED_TOPIC, 3, (short) 1); }

    @Bean NewTopic bureauRequestedTopic() {
        return new NewTopic(BUREAU_REQUESTED_TOPIC, 3, (short) 1);
    }
    @Bean NewTopic bureauCompletedTopic() {
        return new NewTopic(BUREAU_COMPLETED_TOPIC, 3, (short) 1);
    }
}