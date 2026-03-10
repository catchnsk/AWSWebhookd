package com.company.deliverylistener.domain.model;

import lombok.Builder;
import lombok.Value;

import java.time.Duration;
import java.time.Instant;

/**
 * Captures the outcome of a single webhook delivery attempt.
 */
@Value
@Builder
public class DeliveryResult {

    boolean success;

    /** HTTP status code returned by the subscriber endpoint; -1 if no response. */
    int httpStatus;

    /** Human-readable failure reason for logging and header enrichment. */
    String failureReason;

    /** Whether the failure is transient and should be retried. */
    boolean retriable;

    /** Round-trip latency of the outbound HTTP call. */
    Duration latency;

    /** Timestamp of the attempt. */
    Instant attemptedAt;

    public static DeliveryResult success(int httpStatus, Duration latency) {
        return DeliveryResult.builder()
                .success(true)
                .httpStatus(httpStatus)
                .latency(latency)
                .attemptedAt(Instant.now())
                .build();
    }

    public static DeliveryResult retriableFailure(int httpStatus, String reason, Duration latency) {
        return DeliveryResult.builder()
                .success(false)
                .retriable(true)
                .httpStatus(httpStatus)
                .failureReason(reason)
                .latency(latency)
                .attemptedAt(Instant.now())
                .build();
    }

    public static DeliveryResult nonRetriableFailure(int httpStatus, String reason, Duration latency) {
        return DeliveryResult.builder()
                .success(false)
                .retriable(false)
                .httpStatus(httpStatus)
                .failureReason(reason)
                .latency(latency)
                .attemptedAt(Instant.now())
                .build();
    }
}
