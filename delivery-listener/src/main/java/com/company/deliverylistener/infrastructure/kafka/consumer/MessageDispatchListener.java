package com.company.deliverylistener.infrastructure.kafka.consumer;

import com.company.deliverylistener.application.service.DeliveryProcessor;
import com.company.deliverylistener.infrastructure.kafka.headers.RetryHeaders;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Kafka message listener for <em>main</em> topics.
 *
 * <p>Responsibilities:
 * <ol>
 *   <li>Extract or generate a correlation ID and populate the MDC for structured logging.</li>
 *   <li>Delegate to {@link DeliveryProcessor} for subscription lookup and webhook delivery.</li>
 *   <li>Acknowledge the Kafka offset only after the delivery processor has completed
 *       (success, retry-routed, or DL-routed).</li>
 * </ol>
 *
 * <p>The listener itself does <em>not</em> contain retry logic. All retry routing
 * is handled inside {@code DeliveryProcessor}, which calls {@code RetryRoutingService}
 * and {@code DeadLetterPublisher} as needed.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MessageDispatchListener {

    private final DeliveryProcessor deliveryProcessor;

    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String correlationId = resolveCorrelationId(record);
        MDC.put("correlationId", correlationId);
        MDC.put("topic", record.topic());
        MDC.put("partition", String.valueOf(record.partition()));
        MDC.put("offset", String.valueOf(record.offset()));

        try {
            log.debug("Main consumer received record topic={} partition={} offset={}",
                    record.topic(), record.partition(), record.offset());

            deliveryProcessor.process(record, correlationId);

            // Commit offset after processor has handled the message (success or retry-routed)
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Unhandled exception in main listener topic={} – committing offset to avoid infinite redelivery",
                    record.topic(), e);
            // Still acknowledge to prevent infinite redelivery loop; the processor
            // should have already published to the DL topic before throwing.
            acknowledgment.acknowledge();
        } finally {
            MDC.clear();
        }
    }

    private String resolveCorrelationId(ConsumerRecord<String, String> record) {
        var header = record.headers().lastHeader(RetryHeaders.CORRELATION_ID);
        if (header != null && header.value() != null) {
            return new String(header.value());
        }
        return UUID.randomUUID().toString();
    }
}
