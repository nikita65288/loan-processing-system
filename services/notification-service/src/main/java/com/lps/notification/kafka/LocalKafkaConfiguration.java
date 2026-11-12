package com.lps.notification.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;
import java.util.*;

/** Distinct consumer group and typed JSON; no implicit processing-error offset skipping. */
@Configuration
@org.springframework.context.annotation.Import(com.lps.kafka.reliability.KafkaDltSupportConfiguration.class)
@EnableKafka
public class LocalKafkaConfiguration {
    private final com.lps.kafka.reliability.KafkaDltSupport kafkaDltSupport;
    public LocalKafkaConfiguration(com.lps.kafka.reliability.KafkaDltSupport kafkaDltSupport) {
        this.kafkaDltSupport = kafkaDltSupport;
    }

    public static final String REQUESTED="loan.notification.requested";
    public static final String COMPLETED="loan.notification.completed";
    public static final String FAILED="loan.notification.failed";
    @Bean NewTopic requestedTopic() { return new NewTopic(REQUESTED,3,(short)1); }
    @Bean NewTopic completedTopic() { return new NewTopic(COMPLETED,3,(short)1); }
    @Bean NewTopic failedTopic() { return new NewTopic(FAILED,3,(short)1); }
    @Bean
    ConcurrentKafkaListenerContainerFactory<String,com.lps.contracts.command.SendLoanDecisionNotificationCommand> commandListenerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${lps.kafka.consumer.enabled:true}") boolean enabled) {
        Map<String,Object> props=new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"notification-service");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        var deserializer=com.lps.kafka.reliability.KafkaDltSupport.valueDeserializer(com.lps.contracts.command.SendLoanDecisionNotificationCommand.class);
        var factory=new ConcurrentKafkaListenerContainerFactory<String,com.lps.contracts.command.SendLoanDecisionNotificationCommand>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props,new StringDeserializer(),deserializer));
        factory.setAutoStartup(enabled);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setRecordInterceptor(new com.lps.notification.observability.KafkaMdcInterceptor<>());
        factory.setCommonErrorHandler(kafkaDltSupport.newErrorHandler());
        return factory;
    }
    @Bean ProducerFactory<String,String> localProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String brokers) {
        Map<String,Object> props=new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }
    @Bean KafkaTemplate<String,String> localKafkaTemplate(ProducerFactory<String,String> localProducerFactory) {
        var template = new KafkaTemplate<>(localProducerFactory);
        template.setObservationEnabled(true);
        return template;
    }
}
