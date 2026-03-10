package com.company.deliverylistener.application.service;

import com.company.deliverylistener.domain.model.DeliveryResult;
import com.company.deliverylistener.domain.model.EventSubscription;
import com.company.deliverylistener.domain.model.RetryStage;
import com.company.deliverylistener.infrastructure.kafka.headers.RetryHeaders;
import com.company.deliverylistener.infrastructure.kafka.producer.DeadLetterPublisher;
import com.company.deliverylistener.infrastructure.kafka.producer.RetryRoutingPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

/**
 * Determines whether a failed delivery should be retried or dead-lettered,
 * and routes the message accordingly.
 *
 * <p>Routing rules:
 * <ol>
 *   <li>Non-retriable failures → immediately published to DL topic.</li>
 *   <li>Retriable failure at MAIN → published to .R1 with dueTime = now + 3m.</li>
 *   <li>Retriable failure at R1   → published to .R2 with dueTime = now + 6m.</li>
 *   <li>Retriable failure at R2   → published to .R3 with dueTime = now + 9m.</li>
 *   <li>Retriable failure at R3   → published to .DL (retries exhausted).</li>
 * </ol>
 *
 * <p>Per-subscription {@code maxAttempts} overrides the default 3-retry chain when present.
 * (Currently enforced as: if maxAttempts=1, any failure → DL immediately.)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RetryRoutingService {

    private static final int DEFAULT_MAX_ATTEMPTS = 3;

    private final RetryRoutingPublisher retryPublisher;
    private final DeadLetterPublisher   dlPublisher;
    private final DeliveryMetricsService metricsService;

    /**
     * Routes a failed delivery message to the appropriate next destination.
     *
     * @param record         original Kafka record
     * @param subscription   the subscription that failed delivery
     * @param deliveryResult the delivery outcome (must be a failure)
     * @param currentStage   the retry stage at which delivery was attempted
     * @param baseTopic      the main topic name (without suffix)
     * @param correlationId  propagated correlation ID
     */
    public void route(ConsumerRecord<String, String> record,
                      EventSubscription subscription,
                      DeliveryResult deliveryResult,
                      RetryStage currentStage,
                      String baseTopic,
                      String correlationId) {

        if (!deliveryResult.isRetriable()) {
            // Non-retriable – send straight to DL
            log.warn("Non-retriable failure for subscriptionId={} reason={} – routing to DL",
                    subscription.getEventSubscriptionId(), deliveryResult.getFailureReason());
            publishToDl(record, subscription, deliveryResult, baseTopic, correlationId, currentStage);
            metricsService.recordRoutedToDl(baseTopic);
            return;
        }

        // Check per-subscription maxAttempts override
        int maxAttempts = subscription.getMaxAttempts() != null
                ? subscription.getMaxAttempts() : DEFAULT_MAX_ATTEMPTS;

        int currentAttemptCount = parseRetryCount(record) + 1; // this attempt counts

        if (currentStage == RetryStage.R3 || currentAttemptCount >= maxAttempts) {
            log.warn("Retries exhausted for subscriptionId={} currentStage={} attempts={} – routing to DL",
                    subscription.getEventSubscriptionId(), currentStage, currentAttemptCount);
            publishToDl(record, subscription, deliveryResult, baseTopic, correlationId, currentStage);
            metricsService.recordRoutedToDl(baseTopic);
            return;
        }

        // Route to next retry stage
        RetryStage nextStage = currentStage.next();
        log.info("Routing to {} for subscriptionId={} correlationId={}",
                nextStage, subscription.getEventSubscriptionId(), correlationId);

        retryPublisher.routeToNextRetry(
                record, baseTopic, currentStage,
                deliveryResult.getFailureReason(), correlationId,
                subscription.getEventSubscriptionId(),
                subscription.getEventSchemaId(),
                subscription.getSubscriberEntityId());

        metricsService.recordRoutedToRetry(nextStage, baseTopic);
    }

    // -------------------------------------------------------------------------

    private void publishToDl(ConsumerRecord<String, String> record,
                              EventSubscription subscription,
                              DeliveryResult deliveryResult,
                              String baseTopic,
                              String correlationId,
                              RetryStage currentStage) {
        int totalAttempts = parseRetryCount(record) + 1;
        dlPublisher.publish(record, baseTopic, deliveryResult.getFailureReason(),
                correlationId, totalAttempts);
    }

    private static int parseRetryCount(ConsumerRecord<?, ?> record) {
        var h = record.headers().lastHeader(RetryHeaders.RETRY_COUNT);
        if (h == null || h.value() == null) return 0;
        try { return Integer.parseInt(new String(h.value())); }
        catch (NumberFormatException e) { return 0; }
    }
}
