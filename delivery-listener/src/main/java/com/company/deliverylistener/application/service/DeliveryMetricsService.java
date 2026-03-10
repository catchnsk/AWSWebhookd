package com.company.deliverylistener.application.service;

import com.company.deliverylistener.domain.model.DeliveryResult;
import com.company.deliverylistener.domain.model.RetryStage;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Centralised Micrometer metrics recording for all delivery and retry events.
 *
 * <p>Prometheus-compatible metric names follow the {@code delivery.*} namespace.
 * Tag dimensions allow per-topic and per-stage filtering in Grafana dashboards.
 */
@Slf4j
@Service
public class DeliveryMetricsService {

    private final MeterRegistry meterRegistry;
    private final ConcurrentHashMap<String, Timer> latencyTimers = new ConcurrentHashMap<>();

    public DeliveryMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    // =========================================================================
    // Consumption metrics
    // =========================================================================

    public void recordMessageConsumed(String topic, RetryStage stage) {
        Counter.builder("delivery.messages.consumed")
               .tag("topic", topic)
               .tag("stage", stage.name())
               .description("Total messages consumed from MSK")
               .register(meterRegistry)
               .increment();
    }

    // =========================================================================
    // Delivery outcome metrics
    // =========================================================================

    public void recordDeliveryResult(DeliveryResult result, String topic, String subscriptionId) {
        if (result.isSuccess()) {
            Counter.builder("delivery.success")
                   .tag("topic", topic)
                   .tag("subscriptionId", subscriptionId)
                   .description("Successful webhook deliveries")
                   .register(meterRegistry)
                   .increment();
        } else if (result.isRetriable()) {
            Counter.builder("delivery.failure.retriable")
                   .tag("topic", topic)
                   .tag("subscriptionId", subscriptionId)
                   .tag("httpStatus", String.valueOf(result.getHttpStatus()))
                   .description("Retriable delivery failures")
                   .register(meterRegistry)
                   .increment();
        } else {
            Counter.builder("delivery.failure.non_retriable")
                   .tag("topic", topic)
                   .tag("subscriptionId", subscriptionId)
                   .tag("httpStatus", String.valueOf(result.getHttpStatus()))
                   .description("Non-retriable delivery failures (routed to DL)")
                   .register(meterRegistry)
                   .increment();
        }

        // Record outbound latency
        recordLatency(result.getLatency(), topic, subscriptionId);
    }

    // =========================================================================
    // Retry routing metrics
    // =========================================================================

    public void recordRoutedToRetry(RetryStage targetStage, String topic) {
        Counter.builder("delivery.routed.retry")
               .tag("stage", targetStage.name())
               .tag("topic", topic)
               .description("Messages routed to retry topic")
               .register(meterRegistry)
               .increment();
    }

    public void recordRoutedToDl(String topic) {
        Counter.builder("delivery.routed.dl")
               .tag("topic", topic)
               .description("Messages routed to dead-letter topic")
               .register(meterRegistry)
               .increment();
    }

    // =========================================================================
    // Latency timer
    // =========================================================================

    private void recordLatency(Duration latency, String topic, String subscriptionId) {
        if (latency == null) return;
        String key = topic + "|" + subscriptionId;
        latencyTimers.computeIfAbsent(key, k ->
                Timer.builder("delivery.http.latency")
                     .tag("topic", topic)
                     .tag("subscriptionId", subscriptionId)
                     .description("Outbound HTTP delivery latency")
                     .register(meterRegistry))
               .record(latency.toMillis(), TimeUnit.MILLISECONDS);
    }
}
