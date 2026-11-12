package com.lps.kafka.reliability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** End-to-end broker test: malformed bytes are isolated; later valid record advances the same partition. */
@Testcontainers
class KafkaDltKafkaIntegrationTest {
    private static final String TOPIC = "loan.scoring.requested";
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));
    public record Payload(String applicationId) { }

    @Test
    void poisonIsIsolatedBeforeValidRecordIsProcessed() throws Exception {
        String brokers = KAFKA.getBootstrapServers();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1),
                    new NewTopic(TOPIC + ".DLT", 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
        }
        var consumerProps = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        var cf = new DefaultKafkaConsumerFactory<>(consumerProps,
                new StringDeserializer(), KafkaDltSupport.valueDeserializer(Payload.class));
        var props = new ContainerProperties(TOPIC);
        props.setGroupId((String) consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG));
        props.setAckMode(ContainerProperties.AckMode.RECORD);
        var listener = new KafkaMessageListenerContainer<String, Payload>(cf, props);
        var valid = new CountDownLatch(1);
        var processed = new AtomicInteger();
        listener.setupMessageListener((MessageListener<String, Payload>) record -> {
            if (record.value() != null && "app-1".equals(record.value().applicationId())) {
                processed.incrementAndGet();
                valid.countDown();
            }
        });

        var metrics = new SimpleMeterRegistry();
        try (var support = new KafkaDltSupport(brokers, "test", metrics)) {
            listener.setCommonErrorHandler(support.newErrorHandler());
            listener.start();
            try {
                Map<String, Object> producerProps = Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                        ProducerConfig.ACKS_CONFIG, "all");
                try (var producer = new KafkaProducer<String, byte[]>(producerProps)) {
                    producer.send(new ProducerRecord<>(TOPIC, "app-1", "{".getBytes(StandardCharsets.UTF_8)))
                            .get(10, TimeUnit.SECONDS);
                    producer.send(new ProducerRecord<>(TOPIC, "app-1",
                            "{\"applicationId\":\"app-1\"}".getBytes(StandardCharsets.UTF_8)))
                            .get(10, TimeUnit.SECONDS);
                }
                assertTrue(valid.await(25, TimeUnit.SECONDS), "Valid record after poison did not progress");
                assertEquals(1, processed.get());
                Map<String, Object> dltConsumerProps = Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                        ConsumerConfig.GROUP_ID_CONFIG, "dlt-inspect-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
                try (var dlt = new KafkaConsumer<byte[], byte[]>(dltConsumerProps)) {
                    dlt.subscribe(List.of(TOPIC + ".DLT"));
                    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                    boolean found = false;
                    while (!found && System.nanoTime() < deadline) {
                        for (var record : dlt.poll(Duration.ofMillis(500))) {
                            assertEquals("{", new String(record.value(), StandardCharsets.UTF_8));
                            assertEquals("app-1", new String(record.key(), StandardCharsets.UTF_8));
                            assertEquals(TOPIC, new String(record.headers()
                                    .lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC).value(), StandardCharsets.UTF_8));
                            assertEquals(0, ByteBuffer.wrap(record.headers()
                                    .lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION).value()).getInt());
                            assertEquals(0, ByteBuffer.wrap(record.headers()
                                    .lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET).value()).getLong());
                            found = true;
                        }
                    }
                    assertTrue(found, "Poison was not transferred to the DLT");
                }
            } finally {
                listener.stop();
            }
        }
    }
}
