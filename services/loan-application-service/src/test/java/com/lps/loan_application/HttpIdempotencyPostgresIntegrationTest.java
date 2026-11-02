package com.lps.loan_application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.loan_application.repository.LoanApplicationRepository;
import com.lps.loan_application.repository.OutboxEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/** Real PostgreSQL/Liquibase/HTTP tests; no H2 or in-memory synchronization. */
@Testcontainers
@AutoConfigureMockMvc
@SpringBootTest(properties = {
        "lps.scheduling.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.datasource.hikari.connection-timeout=2000"
})
class HttpIdempotencyPostgresIntegrationTest {
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Autowired LoanApplicationRepository applications;
    @Autowired JdbcTemplate jdbc;
    @MockitoSpyBean OutboxEventRepository outbox;

    @BeforeEach
    void clearRows() {
        outbox.deleteAll();
        applications.deleteAll();
    }

    @AfterEach
    void resetSpy() {
        Mockito.reset(outbox);
    }

    private String body(String customer, String amount) {
        return """
                {"customerId":"%s","amount":%s,"term":12,"currency":"EUR"}
                """.formatted(customer, amount);
    }

    private MvcResult send(String key, String body) throws Exception {
        var builder = post("/applications").contentType(MediaType.APPLICATION_JSON).content(body);
        if (key != null) builder.header("Idempotency-Key", key);
        return mvc.perform(builder).andReturn();
    }

    private UUID id(MvcResult result) throws Exception {
        JsonNode json = mapper.readTree(result.getResponse().getContentAsString());
        return UUID.fromString(json.get("id").asText());
    }

    @Test
    void firstAndRepeatedHttpPostReturnStableResponseWithOneOutbox() throws Exception {
        String key = "key-" + UUID.randomUUID();
        String customer = UUID.randomUUID().toString();
        MvcResult first = send(key, body(customer, "10000.0"));
        MvcResult repeated = send(key, body(customer, "10000.00"));
        assertEquals(201, first.getResponse().getStatus());
        assertEquals(201, repeated.getResponse().getStatus());
        assertEquals(id(first), id(repeated));
        assertEquals(first.getResponse().getHeader("Location"), repeated.getResponse().getHeader("Location"));
        assertEquals(first.getResponse().getContentAsString(), repeated.getResponse().getContentAsString());
        assertEquals(1, applications.count());
        assertEquals(1, outbox.count());
        assertEquals(key, jdbc.queryForObject("SELECT idempotency_key FROM loan_applications WHERE id = ?",
                String.class, id(first)));
        assertEquals(64, jdbc.queryForObject("SELECT length(request_fingerprint) FROM loan_applications WHERE id = ?",
                Integer.class, id(first)));
    }

    @Test
    void differentPayloadConflictsButDistinctKeysCreateSeparateOperations() throws Exception {
        String key = "key-" + UUID.randomUUID();
        String customer = UUID.randomUUID().toString();
        assertEquals(201, send(key, body(customer, "10000.00")).getResponse().getStatus());
        MvcResult conflict = send(key, body(customer, "20000.00"));
        assertEquals(409, conflict.getResponse().getStatus());
        assertEquals("IDEMPOTENCY_KEY_CONFLICT", mapper.readTree(
                conflict.getResponse().getContentAsString()).get("error").asText());
        assertEquals(201, send("other-" + UUID.randomUUID(), body(customer, "10000.00"))
                .getResponse().getStatus());
        assertEquals(2, applications.count());
        assertEquals(2, outbox.count());
    }

    @Test
    void headerMissingPreservesLegacyNonIdempotentBehavior() throws Exception {
        String body = body(UUID.randomUUID().toString(), "100.00");
        assertNotEquals(id(send(null, body)), id(send(null, body)));
        assertEquals(2, applications.count());
        assertEquals(2, outbox.count());
    }

    @Test
    void duplicateAfterRolledBackOutboxInsertCanCreateFreshly() throws Exception {
        String key = "key-" + UUID.randomUUID();
        String body = body(UUID.randomUUID().toString(), "100.00");
        doThrow(new IllegalStateException("simulated Outbox write failure"))
                .when(outbox).save(any());
        assertEquals(500, send(key, body).getResponse().getStatus());
        assertEquals(0, applications.count());
        assertEquals(0, outbox.count());
        Mockito.reset(outbox);
        assertEquals(201, send(key, body).getResponse().getStatus());
        assertEquals(1, applications.count());
        assertEquals(1, outbox.count());
    }

    @Test
    void concurrentSameKeyAcrossConnectionsCreatesExactlyOneApplication() throws Exception {
        String key = "key-" + UUID.randomUUID();
        String body = body(UUID.randomUUID().toString(), "10000.00");
        List<MvcResult> results = parallel(8, index -> send(key, body));
        Set<UUID> ids = new HashSet<>();
        for (MvcResult response : results) {
            assertEquals(201, response.getResponse().getStatus(), response.getResponse().getContentAsString());
            ids.add(id(response));
        }
        assertEquals(1, ids.size());
        assertEquals(1, applications.count());
        assertEquals(1, outbox.count());
    }

    @Test
    void concurrentSameKeyDifferentPayloadReturnsConflictNotSecondApplication() throws Exception {
        String key = "key-" + UUID.randomUUID();
        String customer = UUID.randomUUID().toString();
        List<MvcResult> results = parallel(2, index -> send(key,
                body(customer, index == 0 ? "100.00" : "200.00")));
        assertEquals(Set.of(201, 409), Set.of(results.get(0).getResponse().getStatus(),
                results.get(1).getResponse().getStatus()));
        assertEquals(1, applications.count());
        assertEquals(1, outbox.count());
    }

    @FunctionalInterface
    interface Worker {
        MvcResult call(int index) throws Exception;
    }

    /** Start all calls together; a deterministic start barrier avoids sequential testing. */
    private List<MvcResult> parallel(int count, Worker worker) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(count);
        CountDownLatch ready = new CountDownLatch(count);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<MvcResult>> futures = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                final int index = i;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("start timeout");
                    return worker.call(index);
                }));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            List<MvcResult> results = new ArrayList<>();
            for (Future<MvcResult> future : futures) results.add(future.get(30, TimeUnit.SECONDS));
            return results;
        } finally {
            start.countDown();
            pool.shutdownNow();
        }
    }
}
