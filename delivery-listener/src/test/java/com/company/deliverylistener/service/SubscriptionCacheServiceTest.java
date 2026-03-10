package com.company.deliverylistener.service;

import com.company.deliverylistener.application.service.SubscriptionCacheService;
import com.company.deliverylistener.domain.model.*;
import com.company.deliverylistener.domain.repository.SubscriptionRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("SubscriptionCacheService unit tests")
class SubscriptionCacheServiceTest {

    @Mock
    private SubscriptionRepository repository;

    private SubscriptionCacheService cacheService;

    @BeforeEach
    void setUp() {
        cacheService = new SubscriptionCacheService(repository, new SimpleMeterRegistry());
    }

    // =========================================================================
    // Cache refresh tests
    // =========================================================================

    @Test
    @DisplayName("Load on startup populates cache with only ACTIVE subscriptions")
    void loadOnStartup_filtersToActiveOnly() {
        when(repository.findAll()).thenReturn(List.of(
                activeSubscription("sub-1", "Microsoft.cardShip.v1.0"),
                inactiveSubscription("sub-2", "Microsoft.cardShip.v1.0"),
                activeSubscription("sub-3", "Uscb.Microsoft.cardTracker.v1.0")
        ));

        cacheService.loadOnStartup();

        assertThat(cacheService.cacheSize()).isEqualTo(2);
        assertThat(cacheService.findById("sub-1")).isPresent();
        assertThat(cacheService.findById("sub-2")).isEmpty(); // inactive – not in cache
        assertThat(cacheService.findById("sub-3")).isPresent();
    }

    @Test
    @DisplayName("findByTopic returns all active subscriptions for that topic")
    void findByTopic_returnsMatchingSubscriptions() {
        when(repository.findAll()).thenReturn(List.of(
                activeSubscription("sub-1", "Microsoft.cardShip.v1.0"),
                activeSubscription("sub-2", "Microsoft.cardShip.v1.0"), // two subscribers on same topic
                activeSubscription("sub-3", "Uscb.cardTracker.v1.0")
        ));
        cacheService.loadOnStartup();

        List<?> subs = cacheService.findByTopic("Microsoft.cardShip.v1.0");

        assertThat(subs).hasSize(2);
    }

    @Test
    @DisplayName("Inactive subscription is ignored even if topic is active")
    void inactiveSubscriberStatus_ignoredInCache() {
        EventSubscription inactiveSubscriber = EventSubscription.builder()
                .eventSubscriptionId("sub-inactive")
                .eventSchemaId("schema-1")
                .subscriberStatus(SubscriberStatus.INACTIVE) // inactive
                .topicStatus(TopicStatus.ACTIVE)             // but topic is active
                .topicName("Microsoft.cardShip.v1.0")
                .build();

        when(repository.findAll()).thenReturn(List.of(inactiveSubscriber));
        cacheService.loadOnStartup();

        assertThat(cacheService.cacheSize()).isZero();
    }

    @Test
    @DisplayName("Inactive topic is ignored even if subscriber is active")
    void inactiveTopicStatus_ignoredInCache() {
        EventSubscription inactiveTopic = EventSubscription.builder()
                .eventSubscriptionId("sub-topic-inactive")
                .eventSchemaId("schema-1")
                .subscriberStatus(SubscriberStatus.ACTIVE)  // subscriber active
                .topicStatus(TopicStatus.INACTIVE)          // but topic inactive
                .topicName("Microsoft.cardShip.v1.0")
                .build();

        when(repository.findAll()).thenReturn(List.of(inactiveTopic));
        cacheService.loadOnStartup();

        assertThat(cacheService.cacheSize()).isZero();
    }

    @Test
    @DisplayName("Refresh failure retains last known good cache")
    void refreshFailure_retainsLastGoodCache() {
        // First successful load
        when(repository.findAll())
                .thenReturn(List.of(activeSubscription("sub-1", "Microsoft.cardShip.v1.0")))
                .thenThrow(new RuntimeException("DynamoDB unavailable"));

        cacheService.loadOnStartup();
        assertThat(cacheService.cacheSize()).isEqualTo(1);

        // Second call fails – old cache retained
        boolean refreshResult = cacheService.refresh();

        assertThat(refreshResult).isFalse();
        assertThat(cacheService.cacheSize()).isEqualTo(1); // still has old data
        assertThat(cacheService.findById("sub-1")).isPresent();
    }

    @Test
    @DisplayName("Successful refresh replaces old cache with new data")
    void successfulRefresh_replacesOldCache() {
        when(repository.findAll())
                .thenReturn(List.of(activeSubscription("sub-old", "Microsoft.cardShip.v1.0")))
                .thenReturn(List.of(activeSubscription("sub-new", "Uscb.cardTracker.v1.0")));

        cacheService.loadOnStartup();
        assertThat(cacheService.findById("sub-old")).isPresent();

        cacheService.refresh();

        assertThat(cacheService.findById("sub-old")).isEmpty();
        assertThat(cacheService.findById("sub-new")).isPresent();
    }

    @Test
    @DisplayName("activeTopicNames returns distinct topic names from active subscriptions")
    void activeTopicNames_returnsDistinctTopics() {
        when(repository.findAll()).thenReturn(List.of(
                activeSubscription("sub-1", "TopicA"),
                activeSubscription("sub-2", "TopicA"), // duplicate topic
                activeSubscription("sub-3", "TopicB")
        ));
        cacheService.loadOnStartup();

        assertThat(cacheService.activeTopicNames()).containsExactlyInAnyOrder("TopicA", "TopicB");
    }

    // =========================================================================
    // Test helpers
    // =========================================================================

    private static EventSubscription activeSubscription(String id, String topicName) {
        return EventSubscription.builder()
                .eventSubscriptionId(id)
                .eventSchemaId("schema-" + id)
                .subscriberEntityId("entity-" + id)
                .subscriberStatus(SubscriberStatus.ACTIVE)
                .topicStatus(TopicStatus.ACTIVE)
                .topicName(topicName)
                .subscriberDeliveryUrl("https://subscriber.example.com/events")
                .subscriberAuthType(AuthType.NONE)
                .eventName("cardActivation")
                .eventVersion("1.0")
                .build();
    }

    private static EventSubscription inactiveSubscription(String id, String topicName) {
        return activeSubscription(id, topicName).toBuilder()
                .subscriberStatus(SubscriberStatus.INACTIVE)
                .build();
    }
}
