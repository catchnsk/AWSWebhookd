package com.company.deliverylistener.infrastructure.kafka.consumer;

import com.company.deliverylistener.application.service.DeliveryProcessor;
import com.company.deliverylistener.application.service.RetryDueTimeEvaluator;
import com.company.deliverylistener.config.properties.RetryProperties;
import com.company.deliverylistener.domain.model.RetryStage;
import com.company.deliverylistener.infrastructure.kafka.headers.RetryHeaders;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Kafka message listener for <em>retry</em> topics (R1, R2, R3).
 *
 * <p><strong>Due-time handling:</strong>
 * Each retry message carries an {@code X-Next-Due-Time} header (ISO-8601).
 * If the current time is before the due time, the listener calls
 * {@code acknowledgment.nack(cappedDuration)} which:
 * <ol>
 *   <li>Pauses the partition for {@code cappedDuration} (max {@link RetryProperties#getMaxNackDuration()}).</li>
 *   <li>Seeks the offset back to the current record.</li>
 *   <li>Resumes polling after the pause, re-delivering the same record.</li>
 * </ol>
 * The nack duration is capped to avoid {@code max.poll.interval.ms} breaches.
 * Multiple nack cycles may be needed for messages with long remaining delays.
 *
 * <p><strong>On processing:</strong> delegates to the same {@link DeliveryProcessor}
 * used by the main consumer, passing the current retry stage so the processor can
 * escalate to the next retry topic or DL on failure.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetryConsumerListener {

    private final DeliveryProcessor      deliveryProcessor;
    private final RetryDueTimeEvaluator  dueTimeEvaluator;
    private final RetryProperties        retryProperties;

    public void onMessage(ConsumerRecord<String, String> record,
                          Acknowledgment acknowledgment,
                          RetryStage stage) {

        String correlationId = headerStr(record, RetryHeaders.CORRELATION_ID);
        MDC.put("correlationId", correlationId);
        MDC.put("topic",         record.topic());
        MDC.put("retryStage",    stage.name());
        MDC.put("partition",     String.valueOf(record.partition()));
        MDC.put("offset",        String.valueOf(record.offset()));

        try {
            Instant nextDueTime = dueTimeEvaluator.parseDueTime(record);

            if (nextDueTime != null && Instant.now().isBefore(nextDueTime)) {
                // Message not yet due – park the partition briefly and re-seek
                Duration remaining  = Duration.between(Instant.now(), nextDueTime);
                Duration nackPeriod = min(remaining, retryProperties.getMaxNackDuration());

                log.debug("Retry message not due yet. stage={} dueIn={}s – nacking for {}s",
                        stage, remaining.toSeconds(), nackPeriod.toSeconds());

                acknowledgment.nack(nackPeriod);
                return;
            }

            log.info("Processing retry delivery stage={} topic={} retryCount={}",
                    stage, record.topic(), headerStr(record, RetryHeaders.RETRY_COUNT));

            deliveryProcessor.processRetry(record, correlationId, stage);
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Unhandled exception in retry listener stage={} – committing to avoid loop",
                    stage, e);
            acknowledgment.acknowledge();
        } finally {
            MDC.clear();
        }
    }

    // -------------------------------------------------------------------------

    private static String headerStr(ConsumerRecord<?, ?> record, String key) {
        var h = record.headers().lastHeader(key);
        return (h != null && h.value() != null) ? new String(h.value()) : "unknown";
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }
}
