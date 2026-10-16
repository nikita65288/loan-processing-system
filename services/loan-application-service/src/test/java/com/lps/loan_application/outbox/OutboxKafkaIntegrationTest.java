package com.lps.loan_application.outbox;

import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.loan_application.kafka.TestConsumerConfiguration;
import com.lps.loan_application.repository.LoanApplicationRepository;
import com.lps.loan_application.repository.OutboxEventRepository;
import com.lps.loan_application.service.LoanApplicationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@SpringBootTest
@Import(TestConsumerConfiguration.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OutboxKafkaIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16");

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(
                    DockerImageName.parse("apache/kafka:3.8.1")
            );

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.task.scheduling.shutdown.await-termination", () -> "true");
        registry.add("spring.task.scheduling.shutdown.await-termination-period", () -> "5s");
        registry.add("spring.datasource.hikari.connection-timeout", () -> "2000");
        registry.add("lps.outbox.polling-interval-ms", () -> "100");
        registry.add("lps.outbox.batch-size", () -> "10");
        registry.add(
                "lps.kafka.topic.application-created",
                () -> "loan.application.created"
        );
        registry.add(
                "lps.kafka.test.consumer.group-id",
                () -> "outbox-integration-" + UUID.randomUUID()
        );
    }

    @Autowired
    private LoanApplicationService service;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private LoanApplicationRepository loanApplicationRepository;

    @BeforeEach
    void clearState() {
        TestConsumerConfiguration.clearEvents();
        outboxEventRepository.deleteAll();
        loanApplicationRepository.deleteAll();
    }

    @Test
    void shouldPersistPublishAndMarkOutboxEvent() throws Exception {
        CreateLoanApplicationRequest request =
                new CreateLoanApplicationRequest(
                        UUID.randomUUID(),
                        new BigDecimal("10000.00"),
                        12,
                        "EUR"
                );

        var response = service.create(request);

        OutboxEvent created = waitForOutboxEvent(response.id(), Duration.ofSeconds(5));

        ApplicationCreatedEvent received =
                TestConsumerConfiguration.receivedEvents()
                        .poll(10, TimeUnit.SECONDS);

        assertNotNull(received);
        assertEquals(response.id(), received.applicationId());
        assertEquals(created.getId(), received.eventId());

        OutboxEvent published =
                waitForPublishedEvent(created.getId(), Duration.ofSeconds(5));

        assertEquals(OutboxEventStatus.PUBLISHED, published.getStatus());
        assertNotNull(published.getPublishedAt());
    }

    private OutboxEvent waitForOutboxEvent(
            UUID applicationId,
            Duration timeout
    ) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);

        while (Instant.now().isBefore(deadline)) {
            var events = outboxEventRepository.findAll();

            if (!events.isEmpty()) {
                return events.stream()
                        .filter(event -> applicationId.equals(event.getAggregateId()))
                        .findFirst()
                        .orElseThrow();
            }

            Thread.sleep(50);
        }

        fail("Outbox event was not persisted within " + timeout);
        return null;
    }

    private OutboxEvent waitForPublishedEvent(
            UUID eventId,
            Duration timeout
    ) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);

        while (Instant.now().isBefore(deadline)) {
            OutboxEvent event = outboxEventRepository.findById(eventId)
                    .orElseThrow();

            if (event.getStatus() == OutboxEventStatus.PUBLISHED) {
                return event;
            }

            Thread.sleep(50);
        }

        fail("Outbox event was not published within " + timeout);
        return null;
    }
}
