package com.lps.sagaorchestrator.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.sagaorchestrator.kafka.ApplicationCreatedListener;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.outbox.SagaOutboxStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

/** Integration tests use real PostgreSQL transactions and uniqueness constraints. */
@Testcontainers
@SpringBootTest(properties = {
        "lps.scheduling.enabled=false",
        "lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false"
})
class SagaInitializationIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void configuration(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        properties.add("spring.datasource.username", POSTGRES::getUsername);
        properties.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired SagaOrchestrationService service;
    @Autowired ApplicationCreatedListener listener;
    @Autowired SagaRepository sagas;
    @Autowired SagaOutboxRepository outbox;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @MockitoSpyBean SagaOutboxRepository outboxSpy;

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM saga_orchestrator.processed_messages");
        outbox.deleteAll();
        sagas.deleteAll();
    }

    @Test
    void createsSagaAndFirstCommandAtomically() throws Exception {
        ApplicationCreatedEvent event = event();
        service.initialize(event);
        SagaInstance saga = sagas.findAll().get(0);
        assertNotNull(saga.getSagaId());
        assertNotEquals(event.applicationId(), saga.getSagaId());
        assertEquals(event.applicationId(), saga.getApplicationId());
        assertEquals(SagaStatus.IN_PROGRESS, saga.getStatus());
        assertEquals(SagaStep.SCORING, saga.getCurrentStep());
        assertEquals(0L, saga.getVersion());
        var row = outbox.findAll().get(0);
        assertEquals(SagaOutboxStatus.NEW, row.getStatus());
        assertEquals(event.applicationId(), row.getApplicationId());
        PerformCreditScoringCommand command = mapper.readValue(row.getPayload(), PerformCreditScoringCommand.class);
        assertEquals(row.getId(), command.commandId());
        assertEquals(event.correlationId(), command.correlationId());
        assertEquals(event.amount(), command.amount());
        assertEquals(event.term(), command.term());
        assertEquals(event.currency(), command.currency());
    }

    @Test
    void replayDoesNotCreateMoreThanOneSagaOrCommand() {
        ApplicationCreatedEvent event = event();
        listener.consume(record(event));
        listener.consume(record(event));
        assertEquals(1, sagas.count());
        assertEquals(1, outbox.count());
    }

    @Test
    void outboxFailureRollsBackEntireSagaTransaction() {
        doThrow(new IllegalStateException("forced persistence failure"))
                .when(outboxSpy).save(any());
        assertThrows(IllegalStateException.class, () -> service.initialize(event()));
        assertEquals(0, sagas.count());
        assertEquals(0, outbox.count());
    }

    @Test
    void twoConcurrentInitializationsRespectUniqueApplicationConstraint() throws Exception {
        ApplicationCreatedEvent event = event();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Callable<Void> task = () -> {
                start.await();
                listener.consume(record(event));
                return null;
            };
            Future<Void> first = executor.submit(task);
            Future<Void> second = executor.submit(task);
            start.countDown();
            first.get(20, TimeUnit.SECONDS);
            second.get(20, TimeUnit.SECONDS);
            assertEquals(1, sagas.count());
            assertEquals(1, outbox.countByApplicationId(event.applicationId()));
        } finally {
            executor.shutdownNow();
        }
    }

    static ApplicationCreatedEvent event() {
        return new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated",
                Instant.now(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("12000.00"), 24, "EUR");
    }

    static ConsumerRecord<String, ApplicationCreatedEvent> record(ApplicationCreatedEvent event) {
        return new ConsumerRecord<>("loan.application.created", 0, 10L,
                event.applicationId().toString(), event);
    }
}
