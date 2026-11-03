package com.lps.loandisbursement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.command.*;
import com.lps.contracts.event.*;
import com.lps.loandisbursement.disbursement.DisbursementRepository;
import com.lps.loandisbursement.disbursement.DisbursementService;
import com.lps.loandisbursement.kafka.LocalKafkaConfiguration;
import com.lps.loandisbursement.outbox.LocalOutboxPublisher;
import com.lps.loandisbursement.outbox.LocalOutboxRepository;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/** Broker proof: read-only request is consumed, and response crosses the real local Outbox. */
@Testcontainers
@SpringBootTest(properties={"lps.kafka.consumer.enabled=true", "lps.scheduling.enabled=true"})
class DisbursementStatusInquiryKafkaIntegrationTest {
    @Container static final PostgreSQLContainer<?> DB=new PostgreSQLContainer<>("postgres:16");
    @Container static final KafkaContainer KAFKA=
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    @DynamicPropertySource static void configure(DynamicPropertyRegistry props) {
        props.add("spring.datasource.url",DB::getJdbcUrl);
        props.add("spring.datasource.username",DB::getUsername);
        props.add("spring.datasource.password",DB::getPassword);
        props.add("spring.kafka.bootstrap-servers",KAFKA::getBootstrapServers);
    }

    @Autowired DisbursementService payouts;
    @Autowired DisbursementRepository records;
    @Autowired LocalOutboxRepository outbox;
    @Autowired ObjectMapper mapper;
    @Autowired org.springframework.jdbc.core.JdbcTemplate jdbc;
    @Autowired LocalOutboxPublisher publisher;

    @Test void kafkaReadOnlyLookupProducesCommittedResponse() throws Exception {
        UUID app=UUID.randomUUID(),commandId=UUID.randomUUID(),inquiryId=UUID.randomUUID();
        payouts.process(new DisburseLoanCommand(commandId,"DisburseLoan",Instant.now(),app,app,
                new BigDecimal("900.00"),"EUR"));
        var req=new DisbursementStatusInquiryCommand(inquiryId,"DisbursementStatusInquiry",
                Instant.now(),app,app,commandId);
        try(var producer=new KafkaProducer<String,String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class))) {
            producer.send(new ProducerRecord<>(LocalKafkaConfiguration.INQUIRY_REQUESTED,
                    app.toString(),mapper.writeValueAsString(req))).get();
        }
        await().atMost(Duration.ofSeconds(25)).until(() ->
                outbox.findAll().stream().anyMatch(e ->
                        LocalKafkaConfiguration.INQUIRY_RESULT.equals(e.getTopic())));
        var event=outbox.findAll().stream().filter(e ->
                LocalKafkaConfiguration.INQUIRY_RESULT.equals(e.getTopic())).findFirst().orElseThrow();
        assertEquals(1,records.count());
        var parsed=mapper.readValue(event.getPayload(),DisbursementStatusInquiryResultEvent.class);
        assertEquals(DisbursementInquiryStatus.COMPLETED,parsed.status());
        assertEquals(commandId,parsed.disbursementCommandId());
        assertEquals(records.findByApplicationId(app).orElseThrow().getId(),parsed.disbursementId());
        publisher.publishPending();
        try (var consumer = new KafkaConsumer<String,String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,"status-result-proof-"+UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class))) {
            consumer.subscribe(List.of(LocalKafkaConfiguration.INQUIRY_RESULT));
            boolean received=false;
            for(int i=0;i<50 && !received;i++) {
                for(var kafkaRecord:consumer.poll(Duration.ofMillis(250))) {
                    var reply=mapper.readValue(kafkaRecord.value(),DisbursementStatusInquiryResultEvent.class);
                    if(inquiryId.equals(reply.inquiryId())) {
                        assertEquals(app.toString(),kafkaRecord.key());
                        assertEquals(DisbursementInquiryStatus.COMPLETED,reply.status());
                        received=true;
                    }
                }
            }
            assertTrue(received,"Reply Outbox must publish to real Kafka");
        }
        assertEquals(1,jdbc.queryForObject("SELECT count(*) FROM loan_disbursement.processed_status_inquiries",Integer.class));
    }
}
