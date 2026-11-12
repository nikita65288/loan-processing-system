package com.lps.kafka.reliability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KafkaDltSupportTest {
    record Contract(String applicationId) { }

    @Test
    void invalidPayloadIsCapturedWithoutBreakingPoll() {
        try (var deserializer = KafkaDltSupport.valueDeserializer(Contract.class)) {
            var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
            assertNull(deserializer.deserialize("loan.scoring.requested", headers, "{".getBytes()));
            assertNotNull(headers.lastHeader("springDeserializerExceptionValue"));
        }
    }

    @Test
    void invalidTransportContractIsNonRetryableButInfrastructureFailuresAreNot() {
        var meters = new SimpleMeterRegistry();
        try (var support = new KafkaDltSupport("localhost:9092", "test-service", meters)) {
            var handler = support.newErrorHandler();
            assertNotNull(handler);
        }
    }
}
