package com.lps.loan_application.cache;

import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import com.lps.loan_application.repository.LoanApplicationRepository;
import com.lps.loan_application.service.LoanApplicationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** Real PostgreSQL and Redis: proves cache behavior beyond a successful HTTP response. */
@Testcontainers
@AutoConfigureMockMvc
@SpringBootTest(properties = {
        "lps.scheduling.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "lps.cache.loan-application.ttl=2s"
})
class LoanApplicationCacheIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:8.10.2"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired private MockMvc mvc;
    @Autowired private StringRedisTemplate template;
    @Autowired private LoanApplicationService service;
    @MockitoSpyBean private LoanApplicationRepository repository;

    @Test
    void firstGetPopulatesRedisAndSecondGetDoesNotQueryPostgres() throws Exception {
        var created = createApplication();
        String key = key(created.id());
        assertEquals(Boolean.FALSE, template.hasKey(key));
        clearInvocations(repository);

        mvc.perform(get("/applications/{id}", created.id()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(created.id().toString()))
                .andExpect(jsonPath("$.amount").value(10000.12));

        verify(repository, times(1)).findById(created.id());
        String json = template.opsForValue().get(key);
        assertNotNull(json);
        assertTrue(json.contains(created.id().toString()));
        assertTrue(template.getExpire(key) > 0);

        mvc.perform(get("/applications/{id}", created.id()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(created.id().toString()));
        verify(repository, times(1)).findById(created.id());
    }

    @Test
    void expiredEntryIsRestoredOnNextGet() throws Exception {
        var created = createApplication();
        String key = key(created.id());
        mvc.perform(get("/applications/{id}", created.id())).andExpect(status().isOk());
        assertEquals(Boolean.TRUE, template.hasKey(key));

        await().atMost(Duration.ofSeconds(6))
                .until(() -> Boolean.FALSE.equals(template.hasKey(key)));
        clearInvocations(repository);

        mvc.perform(get("/applications/{id}", created.id())).andExpect(status().isOk());
        verify(repository, times(1)).findById(created.id());
        assertEquals(Boolean.TRUE, template.hasKey(key));
    }

    @Test
    void missingApplicationIs404AndNotNegativeCached() throws Exception {
        UUID missing = UUID.randomUUID();
        mvc.perform(get("/applications/{id}", missing)).andExpect(status().isNotFound());
        assertEquals(Boolean.FALSE, template.hasKey(key(missing)));
    }

    @Test
    void corruptCachedPayloadFallsBackToPostgresAndReplacesCache() throws Exception {
        var created = createApplication();
        String key = key(created.id());
        template.opsForValue().set(key, "{invalid", Duration.ofMinutes(1));
        clearInvocations(repository);

        mvc.perform(get("/applications/{id}", created.id())).andExpect(status().isOk());
        verify(repository, times(1)).findById(created.id());
        assertTrue(template.opsForValue().get(key).contains(created.id().toString()));
    }

    @Test
    void cacheCanRecoverAfterDataLossWithoutLosingPostgresRow() throws Exception {
        var created = createApplication();
        String key = key(created.id());
        mvc.perform(get("/applications/{id}", created.id())).andExpect(status().isOk());
        template.delete(key); // Simulates an evicted/lost key; manual test covers real restart.

        mvc.perform(get("/applications/{id}", created.id())).andExpect(status().isOk());
        assertTrue(repository.existsById(created.id()));
        assertEquals(Boolean.TRUE, template.hasKey(key));
    }

    private com.lps.loan_application.dto.LoanApplicationResponse createApplication() {
        return service.create(new CreateLoanApplicationRequest(
                UUID.randomUUID(), new BigDecimal("10000.12"), 12, "EUR"));
    }

    private String key(UUID id) {
        return "lps:loan-application:v1:" + id;
    }
}
