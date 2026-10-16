package com.lps.loan_application.kafka;

import com.lps.contracts.event.ApplicationCreatedEvent;
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
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {
    public static final String APPLICATION_CREATED_TOPIC = "loan.application.created";
    public static final String CONSUMER_GROUP = "loan-application-service";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public NewTopic applicationCreatedTopic() {
        return new NewTopic(APPLICATION_CREATED_TOPIC, 3, (short) 1);
    }

    @Bean
    public ProducerFactory<String, ApplicationCreatedEvent> applicationCreatedProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerProperties());
    }

    @Bean
    public KafkaTemplate<String, ApplicationCreatedEvent> kafkaTemplate(
            ProducerFactory<String, ApplicationCreatedEvent> producerFactory) {
        var template = new KafkaTemplate<>(producerFactory);
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public ConsumerFactory<String, ApplicationCreatedEvent> consumerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
//        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        JsonDeserializer<ApplicationCreatedEvent> deserializer = new JsonDeserializer<>(
                ApplicationCreatedEvent.class,
                false
        );
        deserializer.addTrustedPackages("com.lps.contracts");

        return new DefaultKafkaConsumerFactory<>(
                properties,
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ApplicationCreatedEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, ApplicationCreatedEvent> consumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, ApplicationCreatedEvent>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setRecordInterceptor(new com.lps.loan_application.observability.KafkaMdcInterceptor<>());
        return factory;
    }

    private Map<String, Object> producerProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20_000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return properties;
    }
}
