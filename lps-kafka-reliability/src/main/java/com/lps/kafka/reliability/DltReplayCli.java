package com.lps.kafka.reliability;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** One-shot operator CLI: reads one DLT offset without committing and optionally replays original bytes. */
public final class DltReplayCli {
    private static final Set<String> ALLOWED_TOPICS = Set.of(
            "loan.application.created",
            "loan.scoring.requested", "loan.scoring.completed", "loan.scoring.failed",
            "loan.bureau.check.requested", "loan.bureau.check.completed", "loan.bureau.check.failed",
            "loan.decision.requested", "loan.decision.completed", "loan.decision.failed",
            "loan.disbursement.requested", "loan.disbursement.completed", "loan.disbursement.failed",
            "loan.disbursement.status.inquiry.requested", "loan.disbursement.status.inquiry.result",
            "loan.notification.requested", "loan.notification.completed", "loan.notification.failed");

    private DltReplayCli() { }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Exception e) {
            System.err.println("DLT operation failed: " + e.getClass().getSimpleName()
                    + " (no confirmed business recovery)");
            System.exit(1);
        }
    }

    /** Inspect is the default; execution requires an exact, per-record confirmation. */
    static void run(String[] args) throws Exception {
        Map<String, String> options = parse(args);
        String environment = required(options, "environment");
        if (!Set.of("local", "dev", "test").contains(environment)) {
            throw new IllegalArgumentException("environment must be local/dev/test; production replay requires a separate approved process");
        }
        String bootstrap = required(options, "bootstrap");
        String dltTopic = required(options, "dlt-topic");
        int partition = Integer.parseInt(required(options, "partition"));
        long offset = Long.parseLong(required(options, "offset"));
        if (partition < 0 || offset < 0) throw new IllegalArgumentException("partition and offset must be non-negative");
        String originalTopic = dltTopic.endsWith(".DLT")
                ? dltTopic.substring(0, dltTopic.length() - 4) : "";
        if (!ALLOWED_TOPICS.contains(originalTopic)) throw new IllegalArgumentException("DLT topic is not allowlisted");
        boolean execute = options.containsKey("execute");
        String ref = dltTopic + ":" + partition + ":" + offset;
        if (execute && !ref.equals(options.get("confirm"))) {
            throw new IllegalArgumentException("--execute requires --confirm=" + ref);
        }
        if (!execute && options.containsKey("confirm")) {
            throw new IllegalArgumentException("--confirm without --execute is not permitted");
        }

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "lps-dlt-operator-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        var tp = new TopicPartition(dltTopic, partition);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.assign(List.of(tp));
            consumer.seek(tp, offset);
            ConsumerRecord<byte[], byte[]> record = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
            while (System.nanoTime() < deadline && record == null) {
                for (var candidate : consumer.poll(Duration.ofMillis(500))) {
                    if (candidate.offset() == offset) { record = candidate; break; }
                    if (candidate.offset() > offset) throw new IllegalStateException("DLT offset no longer retained");
                }
            }
            if (record == null) throw new IllegalStateException("DLT record not found at requested offset");
            validateOriginal(record, originalTopic);
            int originalPartition = originalPartition(record.headers());
            long originalOffset = originalOffset(record.headers());
            String key = record.key() == null ? "<null>" : new String(record.key(), StandardCharsets.UTF_8);
            System.out.printf("Environment=%s DLT=%s source=%s:%d:%d key=%s payload_bytes=%d sha256=%s%n",
                    environment, ref, originalTopic, originalPartition, originalOffset,
                    key.length() > 90 ? key.substring(0, 90) : key,
                    record.value() == null ? 0 : record.value().length, digest(record.value()));
            System.out.println("Kafka publication alone cannot confirm downstream business recovery.");
            if (!execute) { System.out.println("INSPECT / DRY-RUN: no message was published."); return; }

            // Invalid/missing original application keys cannot be repaired by unmodified replay.
            if (record.key() == null || record.key().length == 0) {
                throw new IllegalStateException("Missing original key: operator must investigate; replay refused");
            }
            // Do not replay to an unintended destination or a no-longer-existing partition.
            try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
                var metadata = admin.describeTopics(List.of(originalTopic)).allTopicNames().get(10, TimeUnit.SECONDS);
                if (metadata.get(originalTopic).partitions().size() <= originalPartition) {
                    throw new IllegalStateException("Original partition is not present");
                }
            }
            Map<String, Object> producerProps = Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.ACKS_CONFIG, "all",
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20000);
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
                var replay = new ProducerRecord<byte[], byte[]>(originalTopic, originalPartition,
                        record.key(), record.value(), replayHeaders(record.headers()));
                var ack = producer.send(replay).get(25, TimeUnit.SECONDS);
                System.out.printf("REPLAY KAFKA ACK: topic=%s partition=%d offset=%d; verify Inbox/Saga/Disbursement manually%n",
                        ack.topic(), ack.partition(), ack.offset());
            }
        }
    }

    static void validateOriginal(ConsumerRecord<byte[], byte[]> record, String expectedTopic) {
        Header header = record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC);
        if (header == null || !expectedTopic.equals(new String(header.value(), StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("DLT original topic metadata does not match destination allowlist");
        }
        originalPartition(record.headers());
        originalOffset(record.headers());
    }

    static int originalPartition(Headers headers) {
        Header header = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION);
        if (header == null || header.value().length != Integer.BYTES)
            throw new IllegalArgumentException("Invalid original partition header");
        int value = ByteBuffer.wrap(header.value()).getInt();
        if (value < 0) throw new IllegalArgumentException("Negative original partition");
        return value;
    }

    static long originalOffset(Headers headers) {
        Header header = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET);
        if (header == null || header.value().length != Long.BYTES)
            throw new IllegalArgumentException("Invalid original offset header");
        long value = ByteBuffer.wrap(header.value()).getLong();
        if (value < 0) throw new IllegalArgumentException("Negative original offset");
        return value;
    }

    /** Drop DLT/exception metadata; preserve original correlation, tracing and business headers. */
    static Headers replayHeaders(Headers incoming) {
        var filtered = new RecordHeaders();
        for (Header header : incoming) {
            String name = header.key();
            if (name.startsWith("kafka_dlt-") || name.startsWith("springDeserializerException")
                    || name.equals("lps-dlt-isolated-at") || name.equals(KafkaHeaders.DELIVERY_ATTEMPT)) continue;
            filtered.add(name, Arrays.copyOf(header.value(), header.value().length));
        }
        return filtered;
    }

    static Map<String, String> parse(String[] args) {
        var result = new HashMap<String, String>();
        Set<String> allowed = Set.of("environment", "bootstrap", "dlt-topic", "partition", "offset", "execute", "confirm");
        for (String arg : args) {
            if (!arg.startsWith("--")) throw new IllegalArgumentException("Expected --key=value arguments");
            int sep = arg.indexOf('=');
            String key = sep == -1 ? arg.substring(2) : arg.substring(2, sep);
            String value = sep == -1 ? "true" : arg.substring(sep + 1);
            if (!allowed.contains(key) || result.putIfAbsent(key, value) != null)
                throw new IllegalArgumentException("Unknown or repeated argument: " + key);
            if (key.equals("execute") && !value.equals("true"))
                throw new IllegalArgumentException("--execute takes no value");
        }
        return result;
    }

    private static String required(Map<String, String> args, String key) {
        String value = args.get(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing --" + key);
        return value;
    }

    private static String digest(byte[] data) throws Exception {
        return data == null ? "null" : HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }
}
