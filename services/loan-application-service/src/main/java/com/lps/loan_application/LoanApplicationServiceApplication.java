package com.lps.loan_application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import com.lps.loan_application.cache.LoanApplicationCacheProperties;

@SpringBootApplication
@EnableConfigurationProperties(LoanApplicationCacheProperties.class)
public class LoanApplicationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(LoanApplicationServiceApplication.class, args);
    }
}
