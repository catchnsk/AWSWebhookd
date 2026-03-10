package com.company.deliverylistener.service;

import com.company.deliverylistener.application.service.*;
import com.company.deliverylistener.domain.model.*;
import com.company.deliverylistener.infrastructure.http.WebhookDeliveryClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("DeliveryProcessor unit tests")
class DeliveryProcessorTest {

    @Mock private SubscriptionCacheService cacheService;
    @Mock private TopicDiscoveryService    topicDiscoveryService;
    @Mock private WebhookDeliveryClient    webhookClient;
    @Mock private AuthTokenService         authTokenService;
    @Mock private RetryRoutingService      retryRoutingService;

    private DeliveryProcessor deliveryProcessor;

    private static final String TOPIC   = "Microsoft.cardShip.v1.0";
    private static final String CORR_ID = "test-corr-id";

    @BeforeEach
    void setUp() {
        deliveryProcessor = new DeliveryProcessor(
                cacheService, topicDiscoveryService, webhookClient,
                authTokenService, retryRoutingService,
                new DeliveryMetricsService(new SimpleMeterRegistry()));
    }

    // =========================================================================
    // Success path
    // =========================================================================

    @Test
    @DisplayName("Successful delivery on main topic – no retry routing")
    void successOnMainTopic_noRetryRouting() {
        var subscription = activeSubscription("sub-1");
        when(cacheService.findByTopic(TOPIC)).thenReturn(List.of(subscription));
        when(authTokenService.resolveToken(subscription)).thenReturn(null);
        when(webhookClient.deliver(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(DeliveryResult.success(200, Duration.ofMillis(50)));

        deliveryProcessor.process(record(TOPIC), CORR_ID);

        verify(webhookClient, times(1)).deliver(
                eq(subscription.getSubscriberDeliveryUrl()),
                any(), any(), any(), any(), eq(CORR_ID), isNull());
        verifyNoInteractions(retryRoutingService);
    }

    @Test
    @DisplayName("Retriable failure on main topic → calls retryRoutingService.route with MAIN stage")
    void retriableFailureOnMain_callsRetryRouting() {
        var subscription = activeSubscription("sub-1");
        when(cacheService.findByTopic(TOPIC)).thenReturn(List.of(subscription));
        when(authTokenService.resolveToken(any())).thenReturn(null);
        when(webhookClient.deliver(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(DeliveryResult.retriableFailure(503, "Service Unavailable",
                        Duration.ofMillis(100)));

        deliveryProcessor.process(record(TOPIC), CORR_ID);

        verify(retryRoutingService).route(
                any(), eq(subscription), any(DeliveryResult.class),
                eq(RetryStage.MAIN), eq(TOPIC), eq(CORR_ID));
    }

    @Test
    @DisplayName("Non-retriable failure on main topic → calls retryRoutingService.route with MAIN stage")
    void nonRetriableFailureOnMain_callsRetryRouting() {
        var subscription = activeSubscription("sub-1");
        when(cacheService.findByTopic(TOPIC)).thenReturn(List.of(subscription));
        when(authTokenService.resolveToken(any())).thenReturn(null);
        when(webhookClient.deliver(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(DeliveryResult.nonRetriableFailure(404, "Not Found",
                        Duration.ofMillis(50)));

        deliveryProcessor.process(record(TOPIC), CORR_ID);

        verify(retryRoutingService).route(
                any(), eq(subscription), any(DeliveryResult.class),
                eq(RetryStage.MAIN), eq(TOPIC), eq(CORR_ID));
    }

    @Test
    @DisplayName("No active subscriptions for topic – message is discarded silently")
    void noActiveSubscriptions_messageDiscarded() {
        when(cacheService.findByTopic(TOPIC)).thenReturn(List.of());

        deliveryProcessor.process(record(TOPIC), CORR_ID);

        verifyNoInteractions(webhookClient, retryRoutingService);
    }

    @Test
    @DisplayName("Multiple subscriptions on same topic – each delivered independently")
    void multipleSubscriptions_eachDeliveredIndependently() {
        var sub1 = activeSubscription("sub-1");
        var sub2 = activeSubscription("sub-2");
        when(cacheService.findByTopic(TOPIC)).thenReturn(List.of(sub1, sub2));
        when(authTokenService.resolveToken(any())).thenReturn(null);
        when(webhookClient.deliver(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(DeliveryResult.success(200, Duration.ofMillis(20)));

        deliveryProcessor.process(record(TOPIC), CORR_ID);

        verify(webhookClient, times(2)).deliver(any(), any(), any(), any(), any(), any(), any());
    }

    // =========================================================================
    // Retry processing
    // =========================================================================

    @Test
    @DisplayName("Retry delivery uses subscription from cache by ID")
    void retryDelivery_usesSubscriptionById() {
        var sub = activeSubscription("sub-1");
        when(cacheService.findById("sub-1")).thenReturn(Optional.of(sub));
        when(authTokenService.resolveToken(sub)).thenReturn("Bearer token");
        when(webhookClient.deliver(any(), any(), any(), any(), any(), any(), eq("Bearer token")))
                .thenReturn(DeliveryResult.success(200, Duration.ofMillis(30)));
        when(topicDiscoveryService.extractBaseTopic(TOPIC + ".R1")).thenReturn(TOPIC);

        var retryRecord = retryRecord(TOPIC + ".R1", "sub-1");
        deliveryProcessor.processRetry(retryRecord, CORR_ID, RetryStage.R1);

        verify(webhookClient).deliver(any(), any(), any(), any(), eq("sub-1"), any(), any());
    }

    @Test
    @DisplayName("Retry delivery with deactivated subscription – routes to DL")
    void retryDelivery_deactivatedSubscription_routesToDl() {
        when(cacheService.findById("sub-gone")).thenReturn(Optional.empty());
        when(topicDiscoveryService.extractBaseTopic(TOPIC + ".R1")).thenReturn(TOPIC);

        var retryRecord = retryRecord(TOPIC + ".R1", "sub-gone");
        deliveryProcessor.processRetry(retryRecord, CORR_ID, RetryStage.R1);

        verify(retryRoutingService).route(any(), any(), any(),
                eq(RetryStage.R3), eq(TOPIC), eq(CORR_ID));
        verifyNoInteractions(webhookClient);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static EventSubscription activeSubscription(String id) {
        return EventSubscription.builder()
                .eventSubscriptionId(id)
                .eventSchemaId("schema-1")
                .subscriberEntityId("entity-1")
                .topicName(TOPIC)
                .subscriberDeliveryUrl("https://subscriber.example.com/webhook")
                .subscriberAuthType(AuthType.NONE)
                .subscriberStatus(SubscriberStatus.ACTIVE)
                .topicStatus(TopicStatus.ACTIVE)
                .eventName("cardActivation")
                .eventVersion("1.0")
                .maxAttempts(3)
                .build();
    }

    private static ConsumerRecord<String, String> record(String topic) {
        return new ConsumerRecord<>(topic, 0, 0L, "entity-key", "{\"event\":\"cardActivated\"}");
    }

    private static ConsumerRecord<String, String> retryRecord(String topic, String subscriptionId) {
        var rec = new ConsumerRecord<>(topic, 0, 0L, "entity-key", "{\"event\":\"cardActivated\"}");
        rec.headers().add("X-Event-Subscription-Id",
                subscriptionId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return rec;
    }
}
