package com.company.deliverylistener.application.service;

import com.company.deliverylistener.domain.model.DeliveryResult;
import com.company.deliverylistener.domain.model.EventSubscription;
import com.company.deliverylistener.domain.model.RetryStage;
import com.company.deliverylistener.infrastructure.http.WebhookDeliveryClient;
import com.company.deliverylistener.infrastructure.kafka.headers.RetryHeaders;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Orchestrates the end-to-end delivery flow for a consumed Kafka message.
 *
 * <p>Flow for a <strong>main topic</strong> message:
 * <ol>
 *   <li>Look up matching active subscriptions by {@code topicName} in the cache.</li>
 *   <li>For each subscription, resolve auth token if required.</li>
 *   <li>POST payload to {@code subscriberDeliveryUrl}.</li>
 *   <li>On success: record metric, done.</li>
 *   <li>On failure: delegate to {@link RetryRoutingService} which publishes to the
 *       appropriate retry topic or DL.</li>
 * </ol>
 *
 * <p>Flow for a <strong>retry topic</strong> message:
 * Same as above, but the {@code currentStage} is passed to {@link RetryRoutingService}
 * so it can correctly escalate to the next stage on failure.
 *
 * <p>If a topic has <em>multiple</em> active subscriptions (e.g. two subscribers for the
 * same base topic), each is delivered independently.  A failure for one subscription does
 * <em>not</em> affect delivery to others.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryProcessor {

    private final SubscriptionCacheService cacheService;
    private final TopicDiscoveryService    topicDiscoveryService;
    private final WebhookDeliveryClient    webhookDeliveryClient;
    private final AuthTokenService         authTokenService;
    private final RetryRoutingService      retryRoutingService;
    private final DeliveryMetricsService   metricsService;

    // =========================================================================
    // Main-topic processing
    // =========================================================================

    /**
     * Processes a record from a main topic.
     *
     * @param record        consumed Kafka record
     * @param correlationId resolved or generated correlation ID
     */
    public void process(ConsumerRecord<String, String> record, String correlationId) {
        String baseTopic = record.topic(); // main topic has no suffix
        metricsService.recordMessageConsumed(baseTopic, RetryStage.MAIN);

        List<EventSubscription> subscriptions = cacheService.findByTopic(baseTopic);
        if (subscriptions.isEmpty()) {
            log.warn("No active subscriptions found for topic={} – discarding message correlationId={}",
                    baseTopic, correlationId);
            return;
        }

        for (EventSubscription subscription : subscriptions) {
            processDelivery(record, subscription, RetryStage.MAIN, baseTopic, correlationId);
        }
    }

    // =========================================================================
    // Retry-topic processing
    // =========================================================================

    /**
     * Processes a record from a retry topic (R1, R2, or R3).
     *
     * @param record        consumed Kafka record (with retry headers)
     * @param correlationId resolved correlation ID from headers
     * @param currentStage  the retry stage this message was consumed from
     */
    public void processRetry(ConsumerRecord<String, String> record,
                             String correlationId,
                             RetryStage currentStage) {

        metricsService.recordMessageConsumed(record.topic(), currentStage);

        // Recover the subscription ID from headers to look up the exact subscription
        String subscriptionId = headerStr(record, RetryHeaders.SUBSCRIPTION_ID);
        String baseTopic      = topicDiscoveryService.extractBaseTopic(record.topic());

        var subscriptionOpt = cacheService.findById(subscriptionId);
        if (subscriptionOpt.isEmpty()) {
            // Subscription may have been deactivated since the retry was enqueued
            log.warn("Subscription id={} not found in cache (deactivated?); " +
                     "routing to DL correlationId={}", subscriptionId, correlationId);
            // Route to DL as non-retriable since subscription is gone
            DeliveryResult dlResult = DeliveryResult.nonRetriableFailure(
                    -1, "Subscription deactivated or not found", null);
            retryRoutingService.route(record, buildStubSubscription(subscriptionId),
                    dlResult, RetryStage.R3, baseTopic, correlationId);
            return;
        }

        processDelivery(record, subscriptionOpt.get(), currentStage, baseTopic, correlationId);
    }

    // =========================================================================
    // Shared delivery logic
    // =========================================================================

    private void processDelivery(ConsumerRecord<String, String> record,
                                  EventSubscription subscription,
                                  RetryStage currentStage,
                                  String baseTopic,
                                  String correlationId) {
        String subscriptionId = subscription.getEventSubscriptionId();
        log.debug("Attempting delivery subscriptionId={} url={} stage={} correlationId={}",
                subscriptionId, maskUrl(subscription.getSubscriberDeliveryUrl()),
                currentStage, correlationId);

        // Resolve auth token (may return null for NONE auth)
        String bearerToken = authTokenService.resolveToken(subscription);

        DeliveryResult result = webhookDeliveryClient.deliver(
                subscription.getSubscriberDeliveryUrl(),
                record.value(),
                subscription.getEventName(),
                subscription.getEventVersion(),
                subscriptionId,
                correlationId,
                bearerToken);

        metricsService.recordDeliveryResult(result, baseTopic, subscriptionId);

        if (result.isSuccess()) {
            log.info("Delivery successful subscriptionId={} httpStatus={} latencyMs={} correlationId={}",
                    subscriptionId, result.getHttpStatus(),
                    result.getLatency() != null ? result.getLatency().toMillis() : -1,
                    correlationId);
        } else {
            log.warn("Delivery failed subscriptionId={} httpStatus={} retriable={} reason={} correlationId={}",
                    subscriptionId, result.getHttpStatus(), result.isRetriable(),
                    result.getFailureReason(), correlationId);

            retryRoutingService.route(record, subscription, result, currentStage, baseTopic, correlationId);
        }
    }

    // -------------------------------------------------------------------------

    private static String headerStr(ConsumerRecord<?, ?> record, String key) {
        var h = record.headers().lastHeader(key);
        return (h != null && h.value() != null) ? new String(h.value()) : "";
    }

    private static String maskUrl(String url) {
        if (url == null) return "null";
        try {
            var uri = java.net.URI.create(url);
            return uri.getScheme() + "://" + uri.getHost() + "/***";
        } catch (Exception e) { return "***"; }
    }

    /** Creates a minimal stub subscription for routing purposes when the real record is gone. */
    private static EventSubscription buildStubSubscription(String subscriptionId) {
        return EventSubscription.builder()
                .eventSubscriptionId(subscriptionId)
                .eventSchemaId("unknown")
                .subscriberEntityId("unknown")
                .maxAttempts(1) // Force DL immediately
                .build();
    }
}
