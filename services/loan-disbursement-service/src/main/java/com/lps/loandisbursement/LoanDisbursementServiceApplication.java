package com.lps.loandisbursement;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Independent business service; all durable effects belong to its own PostgreSQL schema. */
@SpringBootApplication
@EnableScheduling
public class LoanDisbursementServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(LoanDisbursementServiceApplication.class, args);
    }
}
