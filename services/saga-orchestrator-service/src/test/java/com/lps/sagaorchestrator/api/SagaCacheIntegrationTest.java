package com.lps.sagaorchestrator.api;

import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.contracts.event.BureauCheckCompletedEvent;
import com.lps.contracts.event.DecisionCompletedEvent;
import com.lps.contracts.event.LoanDisbursementCompletedEvent;
import com.lps.contracts.event.LoanNotificationCompletedEvent;
import com.lps.contracts.event.CreditScoringFailedEvent;
import com.lps.contracts.model.Decision;
import com.lps.contracts.model.NotificationType;
import com.lps.sagaorchestrator.retry.SagaStepAttemptRepository;
import com.lps.sagaorchestrator.cache.SagaCacheInvalidation;
import com.lps.sagaorchestrator.cache.SagaStateCache;
import com.lps.sagaorchestrator.saga.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Verifies actual HTTP + PostgreSQL + Redis behavior, including commit/rollback boundaries. */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "lps.scheduling.enabled=false",
        "lps.kafka.consumer.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        "lps.cache.saga.enabled=true",
        "lps.cache.saga.ttl=5s"
})
class SagaCacheIntegrationTest {
    @Container static final PostgreSQLContainer<?> DB = new PostgreSQLContainer<>("postgres:16");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", DB::getJdbcUrl);
        registry.add("spring.datasource.username", DB::getUsername);
        registry.add("spring.datasource.password", DB::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired TestRestTemplate http;
    @Autowired SagaOrchestrationService orchestration;
    @Autowired SagaStateCache cache;
    @Autowired SagaCacheInvalidation invalidation;
    @Autowired StringRedisTemplate redis;
    @Autowired PlatformTransactionManager transactions;
    @Autowired SagaRepository sagas;
    @Autowired SagaStepAttemptRepository attempts;
    @MockitoSpyBean SagaRepository sagaSpy;

    private UUID createSaga() {
        UUID id = UUID.randomUUID();
        orchestration.initialize(new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated",
                Instant.now(), UUID.randomUUID(), id, UUID.randomUUID(),
                new BigDecimal("5000"), 12, "EUR"));
        return id;
    }

    private String endpoint(UUID id) { return "/sagas/applications/" + id; }

    @Test
    void firstGetMissSecondGetHitAnd404() {
        UUID id = createSaga();
        assertFalse(Boolean.TRUE.equals(redis.hasKey(cache.keyFor(id))));
        var first = http.getForEntity(endpoint(id), SagaStatusResponse.class);
        assertEquals(HttpStatus.OK, first.getStatusCode());
        assertEquals(SagaStep.SCORING, first.getBody().currentStep());
        assertNotNull(redis.opsForValue().get(cache.keyFor(id)));
        assertTrue(redis.getExpire(cache.keyFor(id)) > 0);
        var second = http.getForEntity(endpoint(id), SagaStatusResponse.class);
        assertEquals(first.getBody(), second.getBody());
        verify(sagaSpy, times(1)).findByApplicationId(id);
        assertEquals(HttpStatus.NOT_FOUND,
                http.getForEntity(endpoint(UUID.randomUUID()), String.class).getStatusCode());
    }

    @Test
    void scoringTransitionDeletesOnlyCommittedCache() {
        UUID id = createSaga();
        http.getForEntity(endpoint(id), SagaStatusResponse.class);
        assertNotNull(redis.opsForValue().get(cache.keyFor(id)));
        orchestration.handleCreditScoringCompleted(new CreditScoringCompletedEvent(
                UUID.randomUUID(), "CreditScoringCompleted", Instant.now(),
                sagas.findByApplicationId(id).orElseThrow().getCorrelationId(), id, 770, true));
        assertNull(redis.opsForValue().get(cache.keyFor(id)));
        assertEquals(SagaStep.BUREAU, http.getForEntity(endpoint(id), SagaStatusResponse.class)
                .getBody().currentStep());
    }

    @Test
    void rollbackDoesNotEvictCachedSnapshot() {
        UUID id = createSaga();
        http.getForEntity(endpoint(id), SagaStatusResponse.class);
        var original = redis.opsForValue().get(cache.keyFor(id));
        new TransactionTemplate(transactions).executeWithoutResult(status -> {
            var saga = sagas.findByApplicationIdForUpdate(id).orElseThrow();
            saga.failStepTimeout(Instant.now());
            invalidation.afterCommit(id);
            status.setRollbackOnly();
        });
        assertEquals(original, redis.opsForValue().get(cache.keyFor(id)));
        assertEquals(SagaStatus.IN_PROGRESS, sagas.findByApplicationId(id).orElseThrow().getStatus());
    }

    @Test
    void shortTtlExpiresAndNextGetRepopulates() throws Exception {
        UUID id = createSaga();
        http.getForEntity(endpoint(id), SagaStatusResponse.class);
        assertTrue(redis.getExpire(cache.keyFor(id)) > 0);
        // Redis deletes the key on TTL expiry, without changing durable Saga state.
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(8))
                .until(() -> !Boolean.TRUE.equals(redis.hasKey(cache.keyFor(id))));
        assertEquals(HttpStatus.OK, http.getForEntity(endpoint(id), SagaStatusResponse.class).getStatusCode());
        assertNotNull(redis.opsForValue().get(cache.keyFor(id)));
    }

    @Test
    void corruptedValueFallsBackToDatabase() {
        UUID id = createSaga();
        redis.opsForValue().set(cache.keyFor(id), "{garbled", Duration.ofSeconds(5));
        assertEquals(SagaStep.SCORING, http.getForEntity(endpoint(id), SagaStatusResponse.class)
                .getBody().currentStep());
        assertNotEquals("{garbled", redis.opsForValue().get(cache.keyFor(id)));
    }

    /** Every ordinary business transition changes the cached snapshot and schedules post-commit DEL. */
    @Test
    void approvedWorkflowEvictsOnEveryStateTransition() {
        UUID id = createSaga();
        UUID correlation = sagas.findByApplicationId(id).orElseThrow().getCorrelationId();
        assertState(id, SagaStep.SCORING, SagaStatus.IN_PROGRESS);

        orchestration.handleCreditScoringCompleted(new CreditScoringCompletedEvent(
                UUID.randomUUID(), "CreditScoringCompleted", Instant.now(), correlation, id, 770, true));
        assertEvicted(id);
        assertState(id, SagaStep.BUREAU, SagaStatus.IN_PROGRESS);

        orchestration.handleBureauCheckCompleted(new BureauCheckCompletedEvent(
                UUID.randomUUID(), "BureauCheckCompleted", Instant.now(), correlation, id, 740, false, true));
        assertEvicted(id);
        assertState(id, SagaStep.DECISION, SagaStatus.IN_PROGRESS);

        orchestration.handleDecisionCompleted(new DecisionCompletedEvent(
                UUID.randomUUID(), "DecisionCompleted", Instant.now(), correlation, id, Decision.APPROVED, "PASS"));
        assertEvicted(id);
        assertState(id, SagaStep.DISBURSEMENT, SagaStatus.IN_PROGRESS);

        var sagaId = sagas.findByApplicationId(id).orElseThrow().getSagaId();
        var disbursementCommand = attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                sagaId, SagaStep.DISBURSEMENT).orElseThrow().getCommandId();
        orchestration.handleDisbursementCompleted(new LoanDisbursementCompletedEvent(
                UUID.randomUUID(), "LoanDisbursementCompleted", Instant.now(), correlation, id,
                disbursementCommand, UUID.randomUUID(), new BigDecimal("5000"), "EUR"));
        assertEvicted(id);
        assertState(id, SagaStep.NOTIFICATION, SagaStatus.IN_PROGRESS);

        var notificationCommand = attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                sagaId, SagaStep.NOTIFICATION).orElseThrow().getCommandId();
        orchestration.handleNotificationCompleted(new LoanNotificationCompletedEvent(
                UUID.randomUUID(), "LoanNotificationCompleted", Instant.now(), correlation,
                id, notificationCommand, NotificationType.LOAN_APPROVED_AND_DISBURSED));
        assertEvicted(id);
        assertState(id, SagaStep.FINISHED, SagaStatus.COMPLETED);
    }

    @Test
    void permanentFailureInvalidatesFailedSnapshot() {
        UUID id = createSaga();
        UUID correlation = sagas.findByApplicationId(id).orElseThrow().getCorrelationId();
        UUID sagaId = sagas.findByApplicationId(id).orElseThrow().getSagaId();
        assertState(id, SagaStep.SCORING, SagaStatus.IN_PROGRESS);
        UUID commandId = attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                sagaId, SagaStep.SCORING).orElseThrow().getCommandId();
        orchestration.handleFailure(new CreditScoringFailedEvent(UUID.randomUUID(),
                "CreditScoringFailed", Instant.now(), correlation, id, commandId,
                "STUB_FAILURE", false), SagaStep.SCORING);
        assertEvicted(id);
        assertState(id, SagaStep.SCORING, SagaStatus.FAILED);
    }

    private void assertEvicted(UUID id) {
        assertNull(redis.opsForValue().get(cache.keyFor(id)));
    }

    private void assertState(UUID id, SagaStep step, SagaStatus status) {
        var response = http.getForEntity(endpoint(id), SagaStatusResponse.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(step, response.getBody().currentStep());
        assertEquals(status, response.getBody().status());
        assertNotNull(redis.opsForValue().get(cache.keyFor(id)));
    }

}
