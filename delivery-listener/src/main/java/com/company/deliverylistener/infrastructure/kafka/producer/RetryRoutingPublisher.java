package com.company.deliverylistener.infrastructure.kafka.producer;

import com.company.deliverylistener.config.properties.KafkaProperties;
import com.company.deliverylistener.config.properties.RetryProperties;
import com.company.deliverylistener.domain.model.RetryMetadata;
import com.company.deliverylistener.domain.model.RetryStage;
import com.company.deliverylistener.infrastructure.kafka.headers.RetryHeaders;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

/**
 * Publishes failed-delivery events to the appropriate retry topic with enriched headers.
 *
 * <p>Topic derivation:
 * <pre>
 *   stage=MAIN  → baseTopic + ".R1"  (first retry)
 *   stage=R1    → baseTopic + ".R2"
 *   stage=R2    → baseTopic + ".R3"
 *   stage=R3    → routed to {@link DeadLetterPublisher} instead
 * </pre>
 *
 * <p>The message key is preserved (typically {@code subscriberEntityId}) to maintain
 * per-entity ordering within a retry topic.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetryRoutingPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaProperties               kafkaProperties;
    private final RetryProperties               retryProperties;

    /**
     * Routes a failed message to the next retry topic in the chain.
     *
     * @param originalRecord the consumed record that failed delivery
     * @param baseTopic      the original main topic name (without suffix)
     * @param currentStage   the stage at which delivery failed
     * @param failureReason  human-readable description of the failure
     * @param correlationId  propagated correlation ID
     */
    public void routeToNextRetry(ConsumerRecord<String, String> originalRecord,
                                 String baseTopic,
                                 RetryStage currentStage,
                                 String failureReason,
                                 String correlationId,
                                 String eventSubscriptionId,
                                 String eventSchemaId,
                                 String subscriberEntityId) {

        RetryStage nextStage = currentStage.next();
        String nextTopic     = buildRetryTopic(baseTopic, nextStage);
        Instant nextDueTime  = Instant.now().plus(delayFor(nextStage));

        // Build the retry metadata
        String existingFirstFailure = headerStr(originalRecord, RetryHeaders.FIRST_FAILURE_TIME);
        Instant firstFailureTime    = existingFirstFailure.equals("unknown")
                ? Instant.now()
                : Instant.parse(existingFirstFailure);

        int retryCount = parseIntHeader(originalRecord, RetryHeaders.RETRY_COUNT, 0) + 1;

        Headers headers = buildHeaders(
                originalRecord, baseTopic, nextTopic, nextDueTime,
                firstFailureTime, failureReason, retryCount,
                eventSubscriptionId, eventSchemaId, subscriberEntityId, correlationId);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(nextTopic, null, originalRecord.key(),
                        originalRecord.value(), headers);

        kafkaTemplate.send(producerRecord)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish to retry topic={} correlationId={}",
                                nextTopic, correlationId, ex);
                    } else {
                        log.info("Routed failed delivery to retry topic={} dueAt={} retryCount={}",
                                nextTopic, nextDueTime, retryCount);
                    }
                });
    }

    // -------------------------------------------------------------------------

    private String buildRetryTopic(String baseTopic, RetryStage stage) {
        KafkaProperties.TopicNaming n = kafkaProperties.getTopicNaming();
        return switch (stage) {
            case R1 -> baseTopic + n.getRetry1Suffix();
            case R2 -> baseTopic + n.getRetry2Suffix();
            case R3 -> baseTopic + n.getRetry3Suffix();
            case DL -> baseTopic + n.getDlSuffix();
            default -> baseTopic;
        };
    }

    private Duration delayFor(RetryStage stage) {
        RetryProperties.Delay d = retryProperties.getDelay();
        return switch (stage) {
            case R1 -> d.getR1();
            case R2 -> d.getR2();
            case R3 -> d.getR3();
            default -> Duration.ZERO;
        };
    }

    private Headers buildHeaders(ConsumerRecord<String, String> original,
                                  String baseTopic, String nextTopic,
                                  Instant nextDueTime, Instant firstFailureTime,
                                  String failureReason, int retryCount,
                                  String subscriptionId, String schemaId,
                                  String entityId, String correlationId) {
        RecordHeaders headers = new RecordHeaders();
        // Preserve all original headers first, then override/add retry-specific ones
        original.headers().forEach(h -> headers.add(h.key(), h.value()));

        setHeader(headers, RetryHeaders.RETRY_COUNT,         String.valueOf(retryCount));
        setHeader(headers, RetryHeaders.ORIGINAL_TOPIC,      baseTopic);
        setHeader(headers, RetryHeaders.CURRENT_TOPIC,       nextTopic);
        setHeader(headers, RetryHeaders.NEXT_DUE_TIME,       nextDueTime.toString());
        setHeader(headers, RetryHeaders.FIRST_FAILURE_TIME,  firstFailureTime.toString());
        setHeader(headers, RetryHeaders.LAST_FAILURE_REASON, failureReason);
        setHeader(headers, RetryHeaders.SUBSCRIPTION_ID,     subscriptionId);
        setHeader(headers, RetryHeaders.SCHEMA_ID,           schemaId);
        setHeader(headers, RetryHeaders.ENTITY_ID,           entityId);
        setHeader(headers, RetryHeaders.CORRELATION_ID,      correlationId);
        return headers;
    }

    private static void setHeader(RecordHeaders headers, String key, String value) {
        if (value == null) return;
        headers.remove(key);
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String headerStr(ConsumerRecord<?, ?> record, String key) {
        var h = record.headers().lastHeader(key);
        return (h != null && h.value() != null) ? new String(h.value()) : "unknown";
    }

    private static int parseIntHeader(ConsumerRecord<?, ?> record, String key, int defaultValue) {
        String v = headerStr(record, key);
        try { return Integer.parseInt(v); } catch (Exception e) { return defaultValue; }
    }
}
