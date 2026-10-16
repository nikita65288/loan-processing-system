package com.lps.loan_application;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
        classes = LoanApplicationServiceApplication.class,
        properties = {
                "spring.datasource.password=postgres",
                "lps.scheduling.enabled=false",
                "spring.datasource.hikari.connection-timeout=2000"
        }
)
class LoanApplicationServiceApplicationTests {

    @Test
    void contextLoads() {
    }
}
