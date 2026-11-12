package com.lps.kafka.reliability;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;
import org.apache.kafka.common.header.internals.RecordHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Isolates invalid Kafka contracts; retriable failures retain their original offset until recovery. */
public final class KafkaDltSupport implements AutoCloseable {
    private final DefaultKafkaProducerFactory<String, Object> factory;
    private final DeadLetterPublishingRecoverer publisher;
    private final MeterRegistry metrics;
    private final String service;

    public KafkaDltSupport(String brokers, String service, MeterRegistry metrics) {
        this.metrics = metrics;
        this.service = service;
        Map<String, Object> settings = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        var serializers = new LinkedHashMap<Class<?>, org.apache.kafka.common.serialization.Serializer<?>>();
        serializers.put(byte[].class, new ByteArraySerializer());
        var json = new JsonSerializer<>();
        json.setAddTypeInfo(false);
        serializers.put(Object.class, json);
        this.factory = new DefaultKafkaProducerFactory<>(settings, new StringSerializer(),
                new DelegatingByTypeSerializer(serializers, true));
        this.publisher = new DeadLetterPublishingRecoverer(new KafkaTemplate<>(factory),
                (record, error) -> new TopicPartition(record.topic() + ".DLT", -1));
        // A completed send future is required before Spring Kafka can recover/commit this record.
        publisher.setFailIfSendResultIsError(true);
        publisher.setWaitForSendResultTimeout(Duration.ofSeconds(25));
        publisher.excludeHeader(DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.EX_STACKTRACE,
                DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.EX_MSG);
        publisher.setHeadersFunction((record, error) -> {
            var headers = new RecordHeaders();
            headers.add("lps-dlt-isolated-at", Instant.now().toString().getBytes(StandardCharsets.UTF_8));
            return headers;
        });
    }

    /** Delegates JSON decoding but retains the original bytes when a record cannot be decoded. */
    public static <T> ErrorHandlingDeserializer<T> valueDeserializer(Class<T> type) {
        var json = new JsonDeserializer<>(type, false);
        json.addTrustedPackages("com.lps.contracts");
        return new ErrorHandlingDeserializer<>(json);
    }

    /** Only explicit bad transport contracts are DLT-eligible. Database and business failures retry. */
    public DefaultErrorHandler newErrorHandler() {
        ConsumerRecordRecoverer confirmedPublish = (record, error) -> {
            try {
                publisher.accept(record, error);
                metrics.counter("lps.kafka.dlt.published", "service", service,
                        "source_topic", record.topic()).increment();
            } catch (RuntimeException exception) {
                metrics.counter("lps.kafka.dlt.publish.failed", "service", service,
                        "source_topic", record.topic()).increment();
                throw exception;
            }
        };
        // Unbounded, spaced retries preserve the prior reliability policy for transient/business failures.
        var handler = new DefaultErrorHandler(confirmedPublish,
                new FixedBackOff(2000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.addNotRetryableExceptions(InvalidKafkaRecordException.class);
        // An application ClassCastException is not necessarily a bad Kafka record.
        handler.addRetryableExceptions(ClassCastException.class);
        return handler;
    }

    @Override
    public void close() { factory.destroy(); }
}
