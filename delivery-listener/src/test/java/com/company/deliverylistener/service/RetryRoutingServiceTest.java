package com.company.deliverylistener.service;

import com.company.deliverylistener.application.service.DeliveryMetricsService;
import com.company.deliverylistener.application.service.RetryRoutingService;
import com.company.deliverylistener.domain.model.*;
import com.company.deliverylistener.infrastructure.kafka.producer.DeadLetterPublisher;
import com.company.deliverylistener.infrastructure.kafka.producer.RetryRoutingPublisher;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("RetryRoutingService unit tests")
class RetryRoutingServiceTest {

    @Mock private RetryRoutingPublisher retryPublisher;
    @Mock private DeadLetterPublisher   dlPublisher;

    private RetryRoutingService retryRoutingService;

    @BeforeEach
    void setUp() {
        retryRoutingService = new RetryRoutingService(
                retryPublisher, dlPublisher,
                new DeliveryMetricsService(new SimpleMeterRegistry()));
    }

    // =========================================================================
    // Retriable failure routing
    // =========================================================================

    @Test
    @DisplayName("MAIN retriable failure → route to R1")
    void retriableFailureOnMain_routesToR1() {
        var record = record("Microsoft.cardShip.v1.0");
        var sub    = subscription("sub-1", 3);
        var result = DeliveryResult.retriableFailure(503, "Service Unavailable", null);

        retryRoutingService.route(record, sub, result, RetryStage.MAIN,
                "Microsoft.cardShip.v1.0", "corr-1");

        verify(retryPublisher).routeToNextRetry(
                eq(record), eq("Microsoft.cardShip.v1.0"), eq(RetryStage.MAIN),
                anyString(), eq("corr-1"), anyString(), anyString(), anyString());
        verifyNoInteractions(dlPublisher);
    }

    @Test
    @DisplayName("R1 retriable failure → route to R2")
    void retriableFailureOnR1_routesToR2() {
        var record = record("Microsoft.cardShip.v1.0.R1");
        var sub    = subscription("sub-1", 3);
        var result = DeliveryResult.retriableFailure(500, "Internal Error", null);

        retryRoutingService.route(record, sub, result, RetryStage.R1,
                "Microsoft.cardShip.v1.0", "corr-1");

        verify(retryPublisher).routeToNextRetry(
                eq(record), eq("Microsoft.cardShip.v1.0"), eq(RetryStage.R1),
                anyString(), eq("corr-1"), anyString(), anyString(), anyString());
        verifyNoInteractions(dlPublisher);
    }

    @Test
    @DisplayName("R2 retriable failure → route to R3")
    void retriableFailureOnR2_routesToR3() {
        var record = record("Microsoft.cardShip.v1.0.R2");
        var result = DeliveryResult.retriableFailure(503, "Unavailable", null);

        retryRoutingService.route(record, subscription("sub-1", 3), result,
                RetryStage.R2, "Microsoft.cardShip.v1.0", "corr-1");

        verify(retryPublisher).routeToNextRetry(
                eq(record), eq("Microsoft.cardShip.v1.0"), eq(RetryStage.R2),
                anyString(), eq("corr-1"), anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("R3 retriable failure → route to DL (retries exhausted)")
    void retriableFailureOnR3_routesToDl() {
        var record = record("Microsoft.cardShip.v1.0.R3");
        var result = DeliveryResult.retriableFailure(503, "Unavailable", null);

        retryRoutingService.route(record, subscription("sub-1", 3), result,
                RetryStage.R3, "Microsoft.cardShip.v1.0", "corr-1");

        verify(dlPublisher).publish(any(), eq("Microsoft.cardShip.v1.0"),
                anyString(), eq("corr-1"), anyInt());
        verifyNoInteractions(retryPublisher);
    }

    // =========================================================================
    // Non-retriable failure
    // =========================================================================

    @Test
    @DisplayName("Non-retriable failure on MAIN → route directly to DL")
    void nonRetriableFailure_routesDirectlyToDl() {
        var record = record("Microsoft.cardShip.v1.0");
        var result = DeliveryResult.nonRetriableFailure(404, "Not Found", null);

        retryRoutingService.route(record, subscription("sub-1", 3), result,
                RetryStage.MAIN, "Microsoft.cardShip.v1.0", "corr-1");

        verify(dlPublisher).publish(any(), eq("Microsoft.cardShip.v1.0"),
                contains("Not Found"), eq("corr-1"), anyInt());
        verifyNoInteractions(retryPublisher);
    }

    // =========================================================================
    // maxAttempts override
    // =========================================================================

    @Test
    @DisplayName("maxAttempts=1 on subscription → DL immediately on first failure")
    void maxAttempts1_dlImmediately() {
        var record = record("Microsoft.cardShip.v1.0");
        var sub    = subscription("sub-1", 1); // maxAttempts=1
        var result = DeliveryResult.retriableFailure(503, "Unavailable", null);

        retryRoutingService.route(record, sub, result, RetryStage.MAIN,
                "Microsoft.cardShip.v1.0", "corr-1");

        verify(dlPublisher).publish(any(), anyString(), anyString(), anyString(), anyInt());
        verifyNoInteractions(retryPublisher);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static ConsumerRecord<String, String> record(String topic) {
        return new ConsumerRecord<>(topic, 0, 0L, "entity-key", "{\"event\":\"test\"}");
    }

    private static EventSubscription subscription(String id, int maxAttempts) {
        return EventSubscription.builder()
                .eventSubscriptionId(id)
                .eventSchemaId("schema-1")
                .subscriberEntityId("entity-1")
                .topicName("Microsoft.cardShip.v1.0")
                .subscriberDeliveryUrl("https://subscriber.example.com/webhook")
                .subscriberAuthType(AuthType.NONE)
                .subscriberStatus(SubscriberStatus.ACTIVE)
                .topicStatus(TopicStatus.ACTIVE)
                .eventName("cardActivation")
                .eventVersion("1.0")
                .maxAttempts(maxAttempts)
                .build();
    }
}
