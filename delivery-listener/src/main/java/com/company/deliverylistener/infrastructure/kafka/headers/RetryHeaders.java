package com.company.deliverylistener.infrastructure.kafka.headers;

/**
 * Kafka message header key constants used across the retry topology.
 *
 * <p>All values are UTF-8 encoded strings placed on Kafka {@link org.apache.kafka.common.header.Header}s.
 * Timestamps are ISO-8601 strings; integers are decimal strings.
 */
public final class RetryHeaders {

    private RetryHeaders() {}

    // ---- Retry tracking ----
    /** Integer: number of retries attempted so far (0 on initial delivery). */
    public static final String RETRY_COUNT          = "X-Retry-Count";
    /** String: base topic where this event was first consumed. */
    public static final String ORIGINAL_TOPIC       = "X-Original-Topic";
    /** String: topic this message currently resides on. */
    public static final String CURRENT_TOPIC        = "X-Current-Topic";
    /** ISO-8601: earliest time the retry consumer should attempt re-delivery. */
    public static final String NEXT_DUE_TIME        = "X-Next-Due-Time";
    /** ISO-8601: time of the very first delivery failure. */
    public static final String FIRST_FAILURE_TIME   = "X-First-Failure-Time";
    /** String: last human-readable failure reason. */
    public static final String LAST_FAILURE_REASON  = "X-Last-Failure-Reason";

    // ---- Routing identifiers ----
    /** String: eventSubscriptionId from DynamoDB. */
    public static final String SUBSCRIPTION_ID      = "X-Event-Subscription-Id";
    /** String: eventSchemaId from DynamoDB. */
    public static final String SCHEMA_ID            = "X-Event-Schema-Id";
    /** String: subscriberEntityId from DynamoDB. */
    public static final String ENTITY_ID            = "X-Subscriber-Entity-Id";
    /** String: correlation / trace ID propagated across hops. */
    public static final String CORRELATION_ID       = "X-Correlation-Id";

    // ---- Outbound delivery headers (added to HTTP request, not Kafka headers) ----
    public static final String HTTP_CONTENT_TYPE    = "Content-Type";
    public static final String HTTP_CORRELATION_ID  = "X-Correlation-Id";
    public static final String HTTP_EVENT_NAME      = "X-Event-Name";
    public static final String HTTP_EVENT_VERSION   = "X-Event-Version";
    public static final String HTTP_SUBSCRIPTION_ID = "X-Subscription-Id";
}
