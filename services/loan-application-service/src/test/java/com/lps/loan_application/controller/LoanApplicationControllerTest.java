package com.lps.loan_application.controller;

import com.lps.loan_application.dto.LoanApplicationResponse;
import com.lps.loan_application.entity.LoanApplicationStatus;
import com.lps.loan_application.service.LoanApplicationService;
import com.lps.loan_application.service.IdempotentLoanApplicationCreator;
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
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;

@WebMvcTest(LoanApplicationController.class)
class LoanApplicationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private LoanApplicationService service;

    @MockitoBean
    private IdempotentLoanApplicationCreator creator;

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

        when(creator.create(any(), nullable(String.class)))
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
                .andExpect(header().string("Location", "/applications/" + applicationId))
                .andExpect(jsonPath("$.id")
                        .value(applicationId.toString()))
                .andExpect(jsonPath("$.status")
                        .value("CREATED"));
    }
    @Test
    void shouldRejectInvalidOrAmbiguousKey() throws Exception {
        String body = """
                {"customerId":"11111111-1111-4111-8111-111111111111",
                 "amount":10000,"term":12,"currency":"EUR"}
                """;
        for (String invalid : new String[]{"", " bad ", "bad,key", "!invalid", "a".repeat(129)}) {
            mockMvc.perform(post("/applications").contentType(MediaType.APPLICATION_JSON)
                            .header("Idempotency-Key", invalid).content(body))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.error").value("INVALID_IDEMPOTENCY_KEY"));
        }
        mockMvc.perform(post("/applications").contentType(MediaType.APPLICATION_JSON)
                        .header("Idempotency-Key", "first", "second").content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldPassValidKeyToCreator() throws Exception {
        UUID id = UUID.randomUUID();
        when(creator.create(any(), eq("key-123"))).thenReturn(new LoanApplicationResponse(
                id, UUID.fromString("11111111-1111-4111-8111-111111111111"),
                new BigDecimal("10000.00"), 12, "EUR", LoanApplicationStatus.CREATED,
                Instant.now(), Instant.now()));
        mockMvc.perform(post("/applications").contentType(MediaType.APPLICATION_JSON)
                        .header("Idempotency-Key", "key-123")
                        .content("""
                         {"customerId":"11111111-1111-4111-8111-111111111111",
                          "amount":10000,"term":12,"currency":"EUR"}
                        """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(id.toString()));
        verify(creator).create(any(), eq("key-123"));
    }
}
