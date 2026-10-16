package com.lps.loan_application;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = LoanApplicationServiceApplication.class,
        properties = {
                "spring.datasource.password=postgres",
                "lps.scheduling.enabled=false",
                "spring.datasource.hikari.connection-timeout=2000"
        }
)
class InfrastructureConfigurationTests {

    @Autowired
    private Environment environment;

    @Test
    void infrastructureDefaultsAreConfigured() {
        assertThat(environment.getProperty("spring.datasource.url"))
                .isEqualTo("jdbc:postgresql://127.0.0.1:5432/lps?connectTimeout=5&socketTimeout=10");

        assertThat(environment.getProperty("spring.data.redis.host"))
                .isEqualTo("localhost");

        assertThat(environment.getProperty("spring.data.redis.port"))
                .isEqualTo("6379");

        assertThat(environment.getProperty("spring.kafka.bootstrap-servers"))
                .isEqualTo("localhost:9092");
    }
}
