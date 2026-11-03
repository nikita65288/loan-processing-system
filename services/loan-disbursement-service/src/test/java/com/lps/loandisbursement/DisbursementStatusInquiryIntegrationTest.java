package com.lps.loandisbursement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.command.DisburseLoanCommand;
import com.lps.contracts.command.DisbursementStatusInquiryCommand;
import com.lps.contracts.event.DisbursementInquiryStatus;
import com.lps.contracts.event.DisbursementStatusInquiryResultEvent;
import com.lps.loandisbursement.disbursement.*;
import com.lps.loandisbursement.outbox.LocalOutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

/** PostgreSQL integration of read-only lookup, concurrent duplicate inquiries and atomic Outbox. */
@Testcontainers
@SpringBootTest(properties = {"lps.scheduling.enabled=false", "lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false"})
class DisbursementStatusInquiryIntegrationTest {
    @Container static final PostgreSQLContainer<?> DB = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource static void db(DynamicPropertyRegistry props) {
        props.add("spring.datasource.url", DB::getJdbcUrl);
        props.add("spring.datasource.username", DB::getUsername);
        props.add("spring.datasource.password", DB::getPassword);
    }

    @Autowired DisbursementStatusInquiryService inquiries;
    @Autowired DisbursementService payouts;
    @Autowired DisbursementRepository records;
    @Autowired LocalOutboxRepository outbox;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;
    @MockitoSpyBean LocalOutboxRepository spyOutbox;

    @BeforeEach void reset() {
        jdbc.update("DELETE FROM loan_disbursement.processed_status_inquiries");
        jdbc.update("DELETE FROM loan_disbursement.outbox_events");
        jdbc.update("DELETE FROM loan_disbursement.processed_commands");
        jdbc.update("DELETE FROM loan_disbursement.disbursements");
    }

    private DisbursementStatusInquiryCommand request(UUID inquiryId, UUID app, UUID payoutCmd) {
        return new DisbursementStatusInquiryCommand(inquiryId, "DisbursementStatusInquiry",
                Instant.now(), app, app, payoutCmd);
    }

    private DisbursementStatusInquiryResultEvent result() throws Exception {
        return mapper.readValue(outbox.findAll().stream()
                .filter(x -> x.getTopic().endsWith("inquiry.result")).findFirst().orElseThrow().getPayload(),
                DisbursementStatusInquiryResultEvent.class);
    }

    @Test void missingOperationDoesNotCreatePayoutOrFalseFailure() throws Exception {
        var req = request(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        inquiries.inspect(req);
        assertEquals(0, records.count());
        assertEquals(DisbursementInquiryStatus.NOT_FOUND, result().status());
        assertNull(result().disbursementId());
    }

    @Test void existingPayoutIsReportedWithoutAnyNewFinancialEffect() throws Exception {
        UUID app = UUID.randomUUID(), original = UUID.randomUUID();
        payouts.process(new DisburseLoanCommand(original, "DisburseLoan", Instant.now(), app, app,
                new BigDecimal("500.00"), "EUR"));
        long before = records.count();
        inquiries.inspect(request(UUID.randomUUID(), app, original));
        assertEquals(before, records.count());
        var value = result();
        assertEquals(DisbursementInquiryStatus.COMPLETED, value.status());
        assertEquals(original, value.disbursementCommandId());
        assertEquals(records.findByApplicationId(app).orElseThrow().getId(), value.disbursementId());
    }

    @Test void otherOriginalCommandIdIsReportedAsConflict() throws Exception {
        UUID app = UUID.randomUUID();
        payouts.process(new DisburseLoanCommand(UUID.randomUUID(), "DisburseLoan", Instant.now(), app, app,
                new BigDecimal("100.00"), "EUR"));
        inquiries.inspect(request(UUID.randomUUID(), app, UUID.randomUUID()));
        assertEquals(DisbursementInquiryStatus.CONFLICT, result().status());
        assertEquals(1, records.count());
    }

    @Test void duplicateIsOneAtomicReplyAndReusedInquiryIdMustMatchOriginal() throws Exception {
        UUID app = UUID.randomUUID(), cmd = UUID.randomUUID(), id = UUID.randomUUID();
        var request = request(id, app, cmd);
        inquiries.inspect(request);
        inquiries.inspect(request);
        assertEquals(1, outbox.count());
        assertEquals(1, jdbc.queryForObject(
                "SELECT count(*) FROM loan_disbursement.processed_status_inquiries", Integer.class));
        assertThrows(IllegalStateException.class,
                () -> inquiries.inspect(request(id, app, UUID.randomUUID())));
    }

    @Test void responseOutboxFailureRollsBackInbox() {
        var request = request(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        doThrow(new IllegalStateException("injected")).when(spyOutbox).saveAndFlush(any());
        assertThrows(IllegalStateException.class, () -> inquiries.inspect(request));
        assertEquals(0, jdbc.queryForObject(
                "SELECT count(*) FROM loan_disbursement.processed_status_inquiries", Integer.class));
    }

    @Test void twoReplicasProcessingSameInquiryStillProduceOneReply() throws Exception {
        var req = request(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        var pool = Executors.newFixedThreadPool(2);
        var gate = new CountDownLatch(1);
        try {
            var a = pool.submit(() -> { gate.await(); inquiries.inspect(req); return true; });
            var b = pool.submit(() -> { gate.await(); inquiries.inspect(req); return true; });
            gate.countDown();
            assertTrue(a.get(20, TimeUnit.SECONDS));
            assertTrue(b.get(20, TimeUnit.SECONDS));
            assertEquals(1, outbox.count());
        } finally { pool.shutdownNow(); }
    }
}
