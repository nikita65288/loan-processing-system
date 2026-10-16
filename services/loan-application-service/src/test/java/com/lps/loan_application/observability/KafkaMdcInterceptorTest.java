package com.lps.loan_application.observability;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/** A reused Kafka consumer thread must not keep another application identifier. */
class KafkaMdcInterceptorTest {
    @Test void clearsMdcAfterRecord() {
        var interceptor = new KafkaMdcInterceptor<String, String>();
        var consumer = (Consumer<String, String>) mock(Consumer.class);
        String id = UUID.randomUUID().toString();
        var record = new ConsumerRecord<>("loan.application.created", 0, 0L, id, "{}");
        try {
            interceptor.intercept(record, consumer);
            assertEquals(id, MDC.get("applicationId"));
            interceptor.afterRecord(record, consumer);
            assertNull(MDC.get("applicationId"));
        } finally { MDC.remove("applicationId"); }
    }
}
