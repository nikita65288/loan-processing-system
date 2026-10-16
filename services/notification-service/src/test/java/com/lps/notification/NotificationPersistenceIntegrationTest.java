package com.lps.notification;

import com.lps.contracts.command.SendLoanDecisionNotificationCommand;
import com.lps.contracts.model.*;
import com.lps.notification.delivery.*;
import com.lps.notification.outbox.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;
import java.time.Instant;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** Persistent Inbox and Outbox guarantee one simulated delivery per application. */
@Testcontainers
@SpringBootTest(properties={"lps.scheduling.enabled=false","lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false"})
class NotificationPersistenceIntegrationTest {
    @Container static final PostgreSQLContainer<?> DB=new PostgreSQLContainer<>("postgres:16");
    @DynamicPropertySource static void db(DynamicPropertyRegistry props) {
        props.add("spring.datasource.url",DB::getJdbcUrl);
        props.add("spring.datasource.username",DB::getUsername);
        props.add("spring.datasource.password",DB::getPassword);
    }
    @Autowired NotificationService service;
    @Autowired NotificationRepository deliveries;
    @Autowired LocalOutboxRepository outbox;
    @Autowired JdbcTemplate jdbc;
    @BeforeEach void cleanup(){
        jdbc.update("DELETE FROM notification.outbox_events");
        jdbc.update("DELETE FROM notification.processed_commands");
        jdbc.update("DELETE FROM notification.notification_deliveries");
    }
    private SendLoanDecisionNotificationCommand request(UUID id,UUID app,Decision decision) {
        return new SendLoanDecisionNotificationCommand(id,"SendLoanDecisionNotification",Instant.now(),
                app,app,UUID.randomUUID(),decision,decision==Decision.APPROVED
                ?NotificationType.LOAN_APPROVED_AND_DISBURSED:NotificationType.LOAN_REJECTED);
    }
    @Test void approvedAndRejectedAreDurableStubDeliveries(){
        var approved=request(UUID.randomUUID(),UUID.randomUUID(),Decision.APPROVED);
        var rejected=request(UUID.randomUUID(),UUID.randomUUID(),Decision.REJECTED);
        service.process(approved);service.process(rejected);
        service.process(approved);service.process(rejected);
        assertEquals(2,deliveries.count());
        assertEquals(2,outbox.count());
        assertTrue(deliveries.findAll().stream().allMatch(d->d.getStatus().equals("SENT_SIMULATED")));
    }
    @Test void alteredPayloadWithSameCommandIdIsRejected(){
        var original=request(UUID.randomUUID(),UUID.randomUUID(),Decision.REJECTED);
        service.process(original);
        var changed=new SendLoanDecisionNotificationCommand(original.commandId(),original.commandType(),
                original.occurredAt(),original.correlationId(),original.applicationId(),original.customerId(),
                Decision.APPROVED,NotificationType.LOAN_APPROVED_AND_DISBURSED);
        assertThrows(IllegalStateException.class,()->service.process(changed));
        assertEquals(1,deliveries.count());assertEquals(1,outbox.count());
    }
    @Test void invalidCommandHasDurableFailureWithoutFakeDelivery(){
        var bad=new SendLoanDecisionNotificationCommand(UUID.randomUUID(),"SendLoanDecisionNotification",
                Instant.now(),UUID.randomUUID(),UUID.randomUUID(),null,Decision.REJECTED,NotificationType.LOAN_REJECTED);
        service.process(bad);service.process(bad);
        assertEquals(0,deliveries.count());assertEquals(1,outbox.count());
        assertTrue(outbox.findAll().get(0).getPayload().contains("INVALID_NOTIFICATION_INPUT"));
    }
}
