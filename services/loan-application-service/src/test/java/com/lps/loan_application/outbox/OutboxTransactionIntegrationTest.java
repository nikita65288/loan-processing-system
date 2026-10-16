package com.lps.loan_application.outbox;

import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import com.lps.loan_application.repository.LoanApplicationRepository;
import com.lps.loan_application.repository.OutboxEventRepository;
import com.lps.loan_application.service.LoanApplicationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

@Testcontainers
@SpringBootTest(
        properties = {
                "lps.scheduling.enabled=false",
                "spring.datasource.hikari.connection-timeout=2000"
        }
)
class OutboxTransactionIntegrationTest {

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

    @MockitoSpyBean
    private OutboxEventRepository outboxEventRepositorySpy;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAll();
        loanApplicationRepository.deleteAll();
    }

    @Test
    void shouldPersistApplicationAndOutboxInSameTransaction() {
        CreateLoanApplicationRequest request = new CreateLoanApplicationRequest(
                UUID.randomUUID(),
                new BigDecimal("10000.00"),
                12,
                "EUR"
        );

        var response = service.create(request);

        var application = loanApplicationRepository.findById(response.id())
                .orElseThrow();

        var outboxEvent = outboxEventRepository.findAll()
                .stream()
                .findFirst()
                .orElseThrow();

        assertEquals(response.id(), application.getId());
        assertEquals(response.id(), outboxEvent.getAggregateId());
        assertNotEquals(response.id(), outboxEvent.getId());
        assertEquals(OutboxEventStatus.NEW, outboxEvent.getStatus());
        assertNull(outboxEvent.getPublishedAt());
        assertTrue(outboxEvent.getPayload().contains(response.id().toString()));
    }

    @Test
    void shouldRollbackApplicationWhenOutboxSaveFails() {
        Mockito.doThrow(new IllegalStateException("outbox failure"))
                .when(outboxEventRepositorySpy)
                .save(any(OutboxEvent.class));

        CreateLoanApplicationRequest request = new CreateLoanApplicationRequest(
                UUID.randomUUID(),
                new BigDecimal("10000.00"),
                12,
                "EUR"
        );

        assertThrows(
                IllegalStateException.class,
                () -> service.create(request)
        );

        // The generated ID is not exposed by the request, so verify that the
        // transaction did not leave an extra application behind.
        assertEquals(0, loanApplicationRepository.count());
        assertEquals(0, outboxEventRepository.count());

    }
}
