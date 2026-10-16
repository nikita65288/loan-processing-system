package com.lps.loandisbursement;

import com.lps.contracts.command.DisburseLoanCommand;
import com.lps.loandisbursement.disbursement.*;
import com.lps.loandisbursement.outbox.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

/** Real PostgreSQL verifies unique payout, concurrency and business+Outbox rollback. */
@Testcontainers
@SpringBootTest(properties={"lps.scheduling.enabled=false","lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false"})
class DisbursementPersistenceIntegrationTest {
    @Container static final PostgreSQLContainer<?> DB=new PostgreSQLContainer<>("postgres:16");
    @DynamicPropertySource static void db(DynamicPropertyRegistry props) {
        props.add("spring.datasource.url",DB::getJdbcUrl);
        props.add("spring.datasource.username",DB::getUsername);
        props.add("spring.datasource.password",DB::getPassword);
    }
    @Autowired DisbursementService service;
    @Autowired DisbursementRepository records;
    @Autowired LocalOutboxRepository outbox;
    @Autowired JdbcTemplate jdbc;
    @MockitoSpyBean LocalOutboxRepository spyOutbox;
    @BeforeEach void cleanup(){
        jdbc.update("DELETE FROM loan_disbursement.outbox_events");
        jdbc.update("DELETE FROM loan_disbursement.processed_commands");
        jdbc.update("DELETE FROM loan_disbursement.disbursements");
    }
    private DisburseLoanCommand command(UUID id,UUID app,String amount){
        return new DisburseLoanCommand(id,"DisburseLoan",Instant.now(),app,app,
                new BigDecimal(amount),"EUR");
    }
    @Test void duplicateAndConflictingCommandsNeverCreateSecondPayout(){
        UUID app=UUID.randomUUID(),id=UUID.randomUUID();
        var cmd=command(id,app,"4500.00");
        service.process(cmd);
        service.process(cmd);
        service.process(command(UUID.randomUUID(),app,"4500.0"));
        service.process(command(UUID.randomUUID(),app,"9000.00"));
        assertEquals(1,records.count());
        assertEquals(1,outbox.count());
        var payout=records.findByApplicationId(app).orElseThrow();
        assertEquals(0,payout.getAmount().compareTo(new BigDecimal("4500.00")));
        assertEquals("EUR",payout.getCurrency());
        assertEquals("COMPLETED",payout.getStatus());
        assertThrows(IllegalStateException.class,()->service.process(command(id,app,"9000.00")));
        assertEquals(1,records.count());
        assertEquals(1,outbox.count());
        assertEquals(1,jdbc.queryForObject(
                "SELECT count(*) FROM loan_disbursement.processed_commands WHERE outcome='CONFLICT'",
                Integer.class));
    }
    @Test void outboxFailureRollsBackPayoutAndInbox(){
        doThrow(new IllegalStateException("injected outbox failure")).when(spyOutbox).saveAndFlush(any());
        assertThrows(IllegalStateException.class,()->service.process(command(UUID.randomUUID(),UUID.randomUUID(),"100.00")));
        assertEquals(0,records.count());
        assertEquals(0,outbox.count());
        assertEquals(0,jdbc.queryForObject("SELECT count(*) FROM loan_disbursement.processed_commands",Integer.class));
    }
    @Test void concurrentDuplicateIsSerializedByPostgres() throws Exception {
        var cmd=command(UUID.randomUUID(),UUID.randomUUID(),"100.00");
        ExecutorService executor=Executors.newFixedThreadPool(2);
        CountDownLatch start=new CountDownLatch(1);
        Callable<Void> work=()->{start.await();service.process(cmd);return null;};
        try {
            var one=executor.submit(work);var two=executor.submit(work);
            start.countDown();one.get(20,TimeUnit.SECONDS);two.get(20,TimeUnit.SECONDS);
            assertEquals(1,records.count());assertEquals(1,outbox.count());
        } finally {executor.shutdownNow();}
    }
    @Test void invalidInputEmitsStableFailureWithoutPayout(){
        var cmd=command(UUID.randomUUID(),UUID.randomUUID(),"0.00");
        service.process(cmd);service.process(cmd);
        assertEquals(0,records.count());assertEquals(1,outbox.count());
        assertTrue(outbox.findAll().get(0).getPayload().contains("INVALID_DISBURSEMENT_INPUT"));
    }
}
