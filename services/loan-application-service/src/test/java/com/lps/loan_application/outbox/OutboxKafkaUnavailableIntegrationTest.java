package com.lps.loan_application.outbox;

import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import com.lps.loan_application.repository.LoanApplicationRepository;
import com.lps.loan_application.repository.OutboxEventRepository;
import com.lps.loan_application.service.LoanApplicationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@SpringBootTest(
        properties = {
                "spring.kafka.bootstrap-servers=localhost:65534",
                "lps.scheduling.enabled=false",
                "spring.datasource.hikari.connection-timeout=2000"
        }
)
class OutboxKafkaUnavailableIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private LoanApplicationService service;

    @Autowired
    private LoanApplicationRepository loanApplicationRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAll();
        loanApplicationRepository.deleteAll();
    }

    @Test
    void shouldPersistApplicationAndOutboxWhenKafkaIsUnavailable() {
        var request = new CreateLoanApplicationRequest(
                UUID.randomUUID(),
                new BigDecimal("10000.00"),
                12,
                "EUR"
        );

        var response = service.create(request);

        assertTrue(loanApplicationRepository.existsById(response.id()));

        OutboxEvent outboxEvent = outboxEventRepository.findAll()
                .stream()
                .filter(event -> response.id().equals(event.getAggregateId()))
                .findFirst()
                .orElseThrow();

        assertEquals(OutboxEventStatus.NEW, outboxEvent.getStatus());
        assertNull(outboxEvent.getPublishedAt());
    }
}
