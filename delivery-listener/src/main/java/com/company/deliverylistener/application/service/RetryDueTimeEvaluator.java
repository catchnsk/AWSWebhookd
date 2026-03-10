package com.company.deliverylistener.application.service;

import com.company.deliverylistener.infrastructure.kafka.headers.RetryHeaders;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Evaluates the {@code X-Next-Due-Time} header of a retry message.
 *
 * <p>Separated into its own service so it can be unit-tested independently
 * and injected wherever due-time logic is needed.
 */
@Slf4j
@Service
public class RetryDueTimeEvaluator {

    /**
     * Parses the {@code X-Next-Due-Time} header as an {@link Instant}.
     *
     * @param record the Kafka consumer record
     * @return the parsed Instant, or {@code null} if the header is absent or malformed
     */
    public Instant parseDueTime(ConsumerRecord<?, ?> record) {
        var header = record.headers().lastHeader(RetryHeaders.NEXT_DUE_TIME);
        if (header == null || header.value() == null) {
            log.debug("No {} header found on record topic={} partition={} offset={}",
                    RetryHeaders.NEXT_DUE_TIME, record.topic(), record.partition(), record.offset());
            return null;
        }
        try {
            return Instant.parse(new String(header.value()));
        } catch (DateTimeParseException e) {
            log.warn("Malformed {} header '{}' – processing immediately",
                    RetryHeaders.NEXT_DUE_TIME, new String(header.value()));
            return null;
        }
    }

    /**
     * Returns true if the message is ready to be processed (due time reached or no due time set).
     */
    public boolean isDue(ConsumerRecord<?, ?> record) {
        Instant dueTime = parseDueTime(record);
        return dueTime == null || !Instant.now().isBefore(dueTime);
    }
}
