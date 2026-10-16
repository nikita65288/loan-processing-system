package com.lps.loan_application.cache;

import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import com.lps.loan_application.repository.LoanApplicationRepository;
import com.lps.loan_application.repository.OutboxEventRepository;
import com.lps.loan_application.service.LoanApplicationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** PostgreSQL remains usable when the Redis endpoint refuses connections. */
@Testcontainers
@AutoConfigureMockMvc
@SpringBootTest(properties = {
        "lps.scheduling.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.data.redis.host=127.0.0.1",
        "spring.data.redis.port=1",
        "spring.data.redis.connect-timeout=100ms",
        "spring.data.redis.timeout=100ms"
})
class LoanApplicationRedisUnavailableIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired private LoanApplicationService service;
    @Autowired private MockMvc mvc;
    @Autowired private LoanApplicationRepository applications;
    @Autowired private OutboxEventRepository outbox;

    @Test
    void postAndGetWorkWithoutRedisAndOutboxStillPersists() throws Exception {
        var response = service.create(new CreateLoanApplicationRequest(
                UUID.randomUUID(), new BigDecimal("25000.00"), 36, "EUR"));

        assertTrue(applications.existsById(response.id()));
        assertTrue(outbox.findAll().stream()
                .anyMatch(event -> response.id().equals(event.getAggregateId())));

        mvc.perform(get("/applications/{id}", response.id()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(response.id().toString()));
    }
}
