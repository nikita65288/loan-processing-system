package com.lps.loan_application.controller;

import com.lps.loan_application.dto.LoanApplicationResponse;
import com.lps.loan_application.entity.LoanApplicationStatus;
import com.lps.loan_application.service.LoanApplicationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LoanApplicationController.class)
class LoanApplicationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private LoanApplicationService service;

    @Test
    void shouldCreateApplication() throws Exception {

        UUID applicationId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();

        LoanApplicationResponse response =
                new LoanApplicationResponse(
                        applicationId,
                        customerId,
                        new BigDecimal("10000.00"),
                        12,
                        "EUR",
                        LoanApplicationStatus.CREATED,
                        Instant.now(),
                        Instant.now()
                );

        when(service.create(any()))
                .thenReturn(response);

        mockMvc.perform(
                        post("/applications")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("""
                                    {
                                      "customerId": "%s",
                                      "amount": 10000.00,
                                      "term": 12,
                                      "currency": "EUR"
                                    }
                                    """.formatted(customerId))
                )
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id")
                        .value(applicationId.toString()))
                .andExpect(jsonPath("$.status")
                        .value("CREATED"));
    }
}
