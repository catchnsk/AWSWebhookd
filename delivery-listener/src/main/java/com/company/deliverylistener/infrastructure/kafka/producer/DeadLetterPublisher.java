package com.company.deliverylistener.infrastructure.kafka.producer;

import com.company.deliverylistener.config.properties.KafkaProperties;
import com.company.deliverylistener.infrastructure.kafka.headers.RetryHeaders;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Publishes exhausted or non-retriable events to the dead-letter topic.
 *
 * <p>The DL topic ({@code baseTopic.DL}) is produce-only from this service.
 * A separate forensics consumer (outside this service) may read the DL topic
 * for alerting, manual replay, or audit.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeadLetterPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaProperties               kafkaProperties;

    /**
     * Publishes the record to the DL topic with final failure metadata.
     *
     * @param originalRecord    the failed record
     * @param baseTopic         base topic name (DL suffix will be appended)
     * @param failureReason     description of the terminal failure
     * @param correlationId     correlation ID for cross-hop tracing
     * @param totalAttempts     total number of delivery attempts made
     */
    public void publish(ConsumerRecord<String, String> originalRecord,
                        String baseTopic,
                        String failureReason,
                        String correlationId,
                        int totalAttempts) {

        String dlTopic = baseTopic + kafkaProperties.getTopicNaming().getDlSuffix();

        RecordHeaders headers = new RecordHeaders();
        originalRecord.headers().forEach(h -> headers.add(h.key(), h.value()));
        setHeader(headers, RetryHeaders.LAST_FAILURE_REASON, failureReason);
        setHeader(headers, RetryHeaders.CORRELATION_ID,      correlationId);
        setHeader(headers, RetryHeaders.RETRY_COUNT,         String.valueOf(totalAttempts));
        setHeader(headers, "X-DL-Timestamp",                 Instant.now().toString());
        setHeader(headers, "X-DL-Original-Topic",            baseTopic);

        ProducerRecord<String, String> dlRecord = new ProducerRecord<>(
                dlTopic, null, originalRecord.key(), originalRecord.value(), headers);

        kafkaTemplate.send(dlRecord)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("CRITICAL: failed to publish to DL topic={} correlationId={} – " +
                                "message will be lost!", dlTopic, correlationId, ex);
                    } else {
                        log.warn("Published to DL topic={} correlationId={} totalAttempts={}",
                                dlTopic, correlationId, totalAttempts);
                    }
                });
    }

    private static void setHeader(RecordHeaders headers, String key, String value) {
        if (value == null) return;
        headers.remove(key);
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
