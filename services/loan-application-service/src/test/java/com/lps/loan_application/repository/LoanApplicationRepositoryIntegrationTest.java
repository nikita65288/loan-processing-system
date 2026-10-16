package com.lps.loan_application.repository;

import com.lps.loan_application.entity.LoanApplication;
import com.lps.loan_application.entity.LoanApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
@SpringBootTest
class LoanApplicationRepositoryIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void configureProperties(
            DynamicPropertyRegistry registry
    ) {
        registry.add(
                "spring.datasource.url",
                postgres::getJdbcUrl
        );
        registry.add(
                "spring.datasource.username",
                postgres::getUsername
        );
        registry.add(
                "spring.datasource.password",
                postgres::getPassword
        );
    }

    @Autowired
    private LoanApplicationRepository repository;

    @Test
    void shouldPersistAndReadApplication() {

        LoanApplication application =
                new LoanApplication(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        new BigDecimal("10000.00"),
                        12,
                        "EUR"
                );

        LoanApplication saved =
                repository.save(application);

        UUID id = saved.getId();

        LoanApplication loaded =
                repository.findById(id)
                        .orElseThrow();

        assertEquals(id, loaded.getId());
        assertEquals(
                application.getCustomerId(),
                loaded.getCustomerId()
        );
        assertEquals(
                new BigDecimal("10000.00"),
                loaded.getAmount()
        );
        assertEquals(
                LoanApplicationStatus.CREATED,
                loaded.getStatus()
        );
    }
}
