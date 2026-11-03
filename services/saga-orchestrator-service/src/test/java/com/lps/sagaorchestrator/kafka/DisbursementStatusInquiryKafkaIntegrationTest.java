package com.lps.sagaorchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.MessageIdentity;
import com.lps.contracts.command.DisbursementStatusInquiryCommand;
import com.lps.contracts.event.*;
import com.lps.contracts.model.Decision;
import com.lps.sagaorchestrator.outbox.*;
import com.lps.sagaorchestrator.retry.*;
import com.lps.sagaorchestrator.saga.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/** Broker proof: Saga Outbox publishes an inquiry and Kafka result resolves its UNKNOWN state. */
@Testcontainers
@SpringBootTest(properties={"lps.scheduling.enabled=false", "lps.saga.retry.max-attempts=1"})
class DisbursementStatusInquiryKafkaIntegrationTest {
    @Container static final PostgreSQLContainer<?> DB = new PostgreSQLContainer<>("postgres:16");
    @Container static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    @DynamicPropertySource static void configure(DynamicPropertyRegistry props) {
        props.add("spring.datasource.url", DB::getJdbcUrl);
        props.add("spring.datasource.username", DB::getUsername);
        props.add("spring.datasource.password", DB::getPassword);
        props.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired SagaOrchestrationService orchestration;
    @Autowired SagaRetryService retries;
    @Autowired DisbursementStatusInquiryService inquiries;
    @Autowired SagaOutboxPublisher publisher;
    @Autowired SagaOutboxRepository outbox;
    @Autowired SagaRepository sagas;
    @Autowired SagaStepAttemptRepository attempts;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;

    @Test void originalCompletionNeverSentYetInquiryResultResumesWorkflow() throws Exception {
        UUID app=UUID.randomUUID(), corr=UUID.randomUUID();
        Instant now=Instant.now();
        var creation=new ApplicationCreatedEvent(UUID.randomUUID(),"ApplicationCreated",now,
                corr,app,UUID.randomUUID(),new BigDecimal("1000.00"),12,"EUR");
        orchestration.initialize(creation);
        orchestration.handleCreditScoringCompleted(new CreditScoringCompletedEvent(
                UUID.randomUUID(),"CreditScoringCompleted",now,corr,app,740,true));
        orchestration.handleBureauCheckCompleted(new BureauCheckCompletedEvent(
                UUID.randomUUID(),"BureauCheckCompleted",now,corr,app,750,false,true));
        orchestration.handleDecisionCompleted(new DecisionCompletedEvent(UUID.randomUUID(),
                "DecisionCompleted",now,corr,app,Decision.APPROVED,"PASS"));
        UUID sagaId=sagas.findByApplicationId(app).orElseThrow().getSagaId();
        UUID payoutCommandId=attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                sagaId,SagaStep.DISBURSEMENT).orElseThrow().getCommandId();
        Instant expired=Instant.now().minusSeconds(100);
        jdbc.update("""
                UPDATE saga_orchestrator.saga_step_attempts
                SET published_at=?, deadline_at=?, retry_after_at=?
                WHERE saga_id=? AND step='DISBURSEMENT'
                """,Timestamp.from(expired.minusSeconds(80)),Timestamp.from(expired),
                Timestamp.from(expired.plusSeconds(5)),sagaId);
        retries.process(sagaId);
        assertEquals(SagaStatus.REQUIRES_RECONCILIATION,
                sagas.findByApplicationId(app).orElseThrow().getStatus());
        inquiries.initiate(sagaId);
        var inquiryOutbox=outbox.findAll().stream()
                .filter(e -> SagaKafkaConfiguration.DISBURSEMENT_INQUIRY_REQUESTED_TOPIC.equals(e.getTopic()))
                .findFirst().orElseThrow();
        var command=mapper.readValue(inquiryOutbox.getPayload(),DisbursementStatusInquiryCommand.class);
        assertEquals(payoutCommandId,command.disbursementCommandId());

        // Publish via production Saga Outbox, then read the exact Kafka command.
        try(var consumer=new KafkaConsumer<String,String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,"inquiry-proof-"+UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class))) {
            consumer.subscribe(List.of(SagaKafkaConfiguration.DISBURSEMENT_INQUIRY_REQUESTED_TOPIC));
            consumer.poll(Duration.ofMillis(300)); // join group before publishing
            publisher.publishPending();
            await().atMost(Duration.ofSeconds(25)).until(() ->
                    outbox.findById(inquiryOutbox.getId()).orElseThrow().getStatus() == SagaOutboxStatus.PUBLISHED);
            boolean received=false;
            for(int i=0; i<40 && !received; i++) {
                for(var record:consumer.poll(Duration.ofMillis(250))) {
                    var parsed=mapper.readValue(record.value(),DisbursementStatusInquiryCommand.class);
                    if(parsed.inquiryId().equals(command.inquiryId())) {
                        assertEquals(app.toString(),record.key());
                        received=true;
                    }
                }
            }
            assertTrue(received,"Real Kafka must carry the inquiry command");
        }
        // Emulate the remote service's durable reply; its DB lookup is tested separately.
        UUID payoutId=UUID.randomUUID();
        var result=new DisbursementStatusInquiryResultEvent(
                MessageIdentity.resultEventId(command.inquiryId(),"DisbursementStatusInquiryResult"),
                "DisbursementStatusInquiryResult",Instant.now(),corr,app,command.inquiryId(),payoutCommandId,
                DisbursementInquiryStatus.COMPLETED,payoutId,creation.amount(),creation.currency());
        try(var producer=new KafkaProducer<String,String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class))) {
            producer.send(new ProducerRecord<>(SagaKafkaConfiguration.DISBURSEMENT_INQUIRY_RESULT_TOPIC,
                    app.toString(),mapper.writeValueAsString(result))).get();
        }
        await().atMost(Duration.ofSeconds(25)).until(() ->
                sagas.findByApplicationId(app).orElseThrow().getCurrentStep()==SagaStep.NOTIFICATION);
        assertEquals(payoutId,sagas.findByApplicationId(app).orElseThrow().getDisbursementId());
        assertEquals(1,outbox.findAll().stream().filter(e ->
                "SendLoanDecisionNotification".equals(e.getCommandType())
                        && e.getApplicationId().equals(app)).count());
    }
}
