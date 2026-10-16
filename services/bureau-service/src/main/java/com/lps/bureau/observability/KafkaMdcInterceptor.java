package com.lps.bureau.observability;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

/** Adds applicationId from the stable Kafka message key to JSON logs for one listener scope. */
public class KafkaMdcInterceptor<K, V> implements RecordInterceptor<K, V> {
    @Override
    public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        // Remove leftovers even if a preceding handler crashed unexpectedly.
        MDC.remove("applicationId");
        if (record.key() != null) MDC.put("applicationId", record.key().toString());
        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        MDC.remove("applicationId");
    }
}
