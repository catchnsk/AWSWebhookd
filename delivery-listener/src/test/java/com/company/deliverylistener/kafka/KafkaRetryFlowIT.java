package com.company.deliverylistener.kafka;

import com.company.deliverylistener.application.service.*;
import com.company.deliverylistener.config.properties.KafkaProperties;
import com.company.deliverylistener.config.properties.RetryProperties;
import com.company.deliverylistener.domain.model.*;
import com.company.deliverylistener.infrastructure.kafka.headers.RetryHeaders;
import com.company.deliverylistener.infrastructure.kafka.producer.DeadLetterPublisher;
import com.company.deliverylistener.infrastructure.kafka.producer.RetryRoutingPublisher;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka integration test for the full retry flow using Testcontainers.
 *
 * <p>Tests the following scenarios:
 * <ul>
 *   <li>Successful delivery on main topic.</li>
 *   <li>Failure on main topic → message appears on .R1 topic.</li>
 *   <li>Failure on R1 → message appears on .R2 topic.</li>
 *   <li>Failure on R3 → message appears on .DL topic.</li>
 * </ul>
 */
@Testcontainers
@DisplayName("Kafka retry flow integration tests")
class KafkaRetryFlowIT {

    @Container
    static final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    private KafkaTemplate<String, String> kafkaTemplate;
    private RetryRoutingService retryRoutingService;
    private DeliveryMetricsService metricsService;

    static final String BASE_TOPIC = "test.cardShip.v1.0";
    static final String R1_TOPIC   = BASE_TOPIC + ".R1";
    static final String R2_TOPIC   = BASE_TOPIC + ".R2";
    static final String R3_TOPIC   = BASE_TOPIC + ".R3";
    static final String DL_TOPIC   = BASE_TOPIC + ".DL";

    @BeforeEach
    void setUp() {
        // Build a real KafkaTemplate pointing at Testcontainers Kafka
        var producerProps = new HashMap<String, Object>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaContainer.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false); // simplify for tests

        DefaultKafkaProducerFactory<String, String> pf =
                new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(pf);

        // Build KafkaProperties pointing at Testcontainers
        KafkaProperties kafkaProps = new KafkaProperties();
        kafkaProps.setBootstrapServers(kafkaContainer.getBootstrapServers());
        kafkaProps.setSecurityProtocol("PLAINTEXT");

        RetryProperties retryProps = new RetryProperties();
        retryProps.getDelay().setR1(Duration.ofSeconds(1));
        retryProps.getDelay().setR2(Duration.ofSeconds(2));
        retryProps.getDelay().setR3(Duration.ofSeconds(3));

        metricsService = new DeliveryMetricsService(new SimpleMeterRegistry());

        RetryRoutingPublisher retryPublisher = new RetryRoutingPublisher(
                kafkaTemplate, kafkaProps, retryProps);
        DeadLetterPublisher dlPublisher = new DeadLetterPublisher(kafkaTemplate, kafkaProps);

        retryRoutingService = new RetryRoutingService(retryPublisher, dlPublisher, metricsService);
    }

    @Test
    @DisplayName("Retriable failure on MAIN publishes message to .R1 topic with headers")
    void mainRetriableFailure_publishesToR1() throws Exception {
        var record = rawRecord(BASE_TOPIC);
        var sub    = subscription("sub-1");
        var result = DeliveryResult.retriableFailure(503, "Service Unavailable", null);

        retryRoutingService.route(record, sub, result, RetryStage.MAIN, BASE_TOPIC, "corr-test");

        var consumed = consumeFromTopic(R1_TOPIC, 1, 10);
        assertThat(consumed).hasSize(1);

        var headers = consumed.get(0).headers();
        assertHeader(headers, RetryHeaders.ORIGINAL_TOPIC, BASE_TOPIC);
        assertHeader(headers, RetryHeaders.RETRY_COUNT, "1");
        assertThat(headerVal(headers, RetryHeaders.NEXT_DUE_TIME)).isNotNull();
        assertHeader(headers, RetryHeaders.CORRELATION_ID, "corr-test");
    }

    @Test
    @DisplayName("Retriable failure on R1 publishes message to .R2 topic")
    void r1RetriableFailure_publishesToR2() throws Exception {
        var record = rawRecordWithRetryCount(R1_TOPIC, 1);
        var result = DeliveryResult.retriableFailure(500, "Internal Error", null);

        retryRoutingService.route(record, subscription("sub-1"), result,
                RetryStage.R1, BASE_TOPIC, "corr-test");

        var consumed = consumeFromTopic(R2_TOPIC, 1, 10);
        assertThat(consumed).hasSize(1);
        assertHeader(consumed.get(0).headers(), RetryHeaders.RETRY_COUNT, "2");
    }

    @Test
    @DisplayName("Retriable failure on R3 publishes message to .DL topic")
    void r3RetriableFailure_publishesToDl() throws Exception {
        var record = rawRecordWithRetryCount(R3_TOPIC + ".R3", 3);
        var result = DeliveryResult.retriableFailure(503, "Final failure", null);

        retryRoutingService.route(record, subscription("sub-1"), result,
                RetryStage.R3, BASE_TOPIC, "corr-dl");

        var consumed = consumeFromTopic(DL_TOPIC, 1, 10);
        assertThat(consumed).hasSize(1);
    }

    @Test
    @DisplayName("Non-retriable failure on MAIN publishes to .DL directly")
    void nonRetriableFailure_publishesToDl() throws Exception {
        var record = rawRecord(BASE_TOPIC);
        var result = DeliveryResult.nonRetriableFailure(404, "Not Found", null);

        retryRoutingService.route(record, subscription("sub-1"), result,
                RetryStage.MAIN, BASE_TOPIC, "corr-nonretriable");

        var consumed = consumeFromTopic(DL_TOPIC, 1, 10);
        assertThat(consumed).hasSize(1);
        assertThat(headerVal(consumed.get(0).headers(), "X-Last-Failure-Reason"))
                .contains("Not Found");
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private List<ConsumerRecord<String, String>> consumeFromTopic(
            String topic, int expectedCount, int timeoutSeconds) throws InterruptedException {

        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            List<ConsumerRecord<String, String>> collected = new ArrayList<>();
            long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);
            while (collected.size() < expectedCount && System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));
                records.forEach(collected::add);
            }
            return collected;
        }
    }

    private ConsumerRecord<String, String> rawRecord(String topic) {
        return new ConsumerRecord<>(topic, 0, 0L, "entity-key", "{\"event\":\"test\"}");
    }

    private ConsumerRecord<String, String> rawRecordWithRetryCount(String topic, int count) {
        var rec = rawRecord(topic);
        rec.headers().add(RetryHeaders.RETRY_COUNT,
                String.valueOf(count).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        rec.headers().add(RetryHeaders.FIRST_FAILURE_TIME,
                java.time.Instant.now().toString()
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return rec;
    }

    private static EventSubscription subscription(String id) {
        return EventSubscription.builder()
                .eventSubscriptionId(id)
                .eventSchemaId("schema-1")
                .subscriberEntityId("entity-1")
                .topicName(BASE_TOPIC)
                .subscriberDeliveryUrl("https://subscriber.example.com/webhook")
                .subscriberAuthType(AuthType.NONE)
                .subscriberStatus(SubscriberStatus.ACTIVE)
                .topicStatus(TopicStatus.ACTIVE)
                .eventName("cardActivation")
                .eventVersion("1.0")
                .maxAttempts(3)
                .build();
    }

    private static void assertHeader(
            org.apache.kafka.common.header.Headers headers, String key, String expectedValue) {
        assertThat(headerVal(headers, key)).isEqualTo(expectedValue);
    }

    private static String headerVal(org.apache.kafka.common.header.Headers headers, String key) {
        var h = headers.lastHeader(key);
        return (h != null && h.value() != null) ? new String(h.value()) : null;
    }
}
