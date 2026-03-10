package com.company.deliverylistener.domain.model;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

/**
 * Retry tracking state carried in Kafka message headers across retry hops.
 *
 * <p>These fields are serialised as UTF-8 string headers on the Kafka message.
 * See {@link com.company.deliverylistener.infrastructure.kafka.headers.RetryHeaders}
 * for the header key constants.
 */
@Value
@Builder(toBuilder = true)
public class RetryMetadata {

    /** Number of delivery attempts made so far (1 on first retry). */
    int retryCount;

    /** The original main topic this event arrived on. */
    String originalTopic;

    /** The topic this message is currently on. */
    String currentTopic;

    /** When (UTC) the retry consumer should attempt re-delivery. */
    Instant nextDueTime;

    /** Timestamp of the very first delivery failure. */
    Instant firstFailureTime;

    /** Human-readable description of the most recent failure. */
    String lastFailureReason;

    /** Routing identifiers – copied from the EventSubscription. */
    String eventSubscriptionId;
    String eventSchemaId;
    String subscriberEntityId;

    /** Cross-cutting correlation ID propagated through all hops. */
    String correlationId;

    /** Current retry stage derived from the topic suffix. */
    RetryStage retryStage;

    public static RetryMetadata initial(
            String originalTopic,
            String correlationId,
            String eventSubscriptionId,
            String eventSchemaId,
            String subscriberEntityId,
            String firstFailureReason,
            Instant nextDueTime) {

        return RetryMetadata.builder()
                .retryCount(1)
                .originalTopic(originalTopic)
                .currentTopic(originalTopic)
                .nextDueTime(nextDueTime)
                .firstFailureTime(Instant.now())
                .lastFailureReason(firstFailureReason)
                .eventSubscriptionId(eventSubscriptionId)
                .eventSchemaId(eventSchemaId)
                .subscriberEntityId(subscriberEntityId)
                .correlationId(correlationId)
                .retryStage(RetryStage.R1)
                .build();
    }
}
