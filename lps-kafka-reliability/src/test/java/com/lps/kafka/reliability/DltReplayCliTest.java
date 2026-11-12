package com.lps.kafka.reliability;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.*;

class DltReplayCliTest {
    @Test
    void replayPreservesBusinessHeadersAndRemovesTransportMetadata() {
        var original = new RecordHeaders();
        original.add("traceparent", "00-correlation".getBytes(StandardCharsets.UTF_8));
        original.add("kafka_dlt-exception-fqcn", "java.lang.Exception".getBytes(StandardCharsets.UTF_8));
        original.add("springDeserializerExceptionValue", new byte[]{4, 5, 6});
        original.add("lps-dlt-isolated-at", "now".getBytes(StandardCharsets.UTF_8));
        var result = DltReplayCli.replayHeaders(original);
        assertNotNull(result.lastHeader("traceparent"));
        assertNull(result.lastHeader("kafka_dlt-exception-fqcn"));
        assertNull(result.lastHeader("springDeserializerExceptionValue"));
        assertNull(result.lastHeader("lps-dlt-isolated-at"));
    }

    @Test
    void originalDestinationMustMatchDltTopic() {
        var headers = new RecordHeaders();
        headers.add(KafkaHeaders.DLT_ORIGINAL_TOPIC, "loan.scoring.requested".getBytes(StandardCharsets.UTF_8));
        headers.add(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(4).putInt(1).array());
        headers.add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(8).putLong(42).array());
        var record = new ConsumerRecord<byte[],byte[]>("loan.scoring.requested.DLT", 0, 0,
                null, "{}".getBytes(StandardCharsets.UTF_8));
        for (var h : headers) record.headers().add(h);
        assertDoesNotThrow(() -> DltReplayCli.validateOriginal(record, "loan.scoring.requested"));
        assertThrows(IllegalArgumentException.class,
                () -> DltReplayCli.validateOriginal(record, "loan.disbursement.requested"));
        assertEquals(1, DltReplayCli.originalPartition(record.headers()));
        assertEquals(42, DltReplayCli.originalOffset(record.headers()));
    }

    @Test
    void duplicateOrUnknownArgumentsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> DltReplayCli.parse(new String[]{"--execute", "--execute"}));
        assertThrows(IllegalArgumentException.class, () -> DltReplayCli.parse(new String[]{"--other=value"}));
    }
}
