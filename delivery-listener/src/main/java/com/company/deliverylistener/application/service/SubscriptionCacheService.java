package com.company.deliverylistener.application.service;

import com.company.deliverylistener.domain.model.EventSubscription;
import com.company.deliverylistener.domain.repository.SubscriptionRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Thread-safe in-memory cache of active {@link EventSubscription} records.
 *
 * <p>The cache is the single source of truth for subscription metadata at runtime.
 * It is refreshed automatically every 24 hours by {@link SubscriptionRefreshScheduler}
 * and can be force-refreshed via the admin API.
 *
 * <p><strong>Fallback behaviour</strong>: if a refresh fails (DynamoDB unavailable, etc.),
 * the previous snapshot is retained and the failure is recorded in metrics.
 * A warning is logged on every subsequent poll until a successful refresh occurs.
 *
 * <p><strong>Thread safety</strong>: all internal maps are replaced atomically on refresh
 * via an {@link AtomicReference} swap. Readers see either the old or new map, never a
 * partially-constructed state.
 */
@Slf4j
@Service
public class SubscriptionCacheService {

    private final SubscriptionRepository repository;
    private final MeterRegistry          meterRegistry;

    /** Atomic snapshot – swapped on every successful refresh. */
    private final AtomicReference<CacheSnapshot> snapshot = new AtomicReference<>(CacheSnapshot.empty());

    public SubscriptionCacheService(SubscriptionRepository repository, MeterRegistry meterRegistry) {
        this.repository    = repository;
        this.meterRegistry = meterRegistry;

        // Expose cache size as a live gauge
        Gauge.builder("delivery.cache.size", snapshot, s -> s.get().bySubscriptionId.size())
             .description("Number of active subscriptions in the in-memory cache")
             .register(meterRegistry);
    }

    @PostConstruct
    public void loadOnStartup() {
        refresh();
    }

    // =========================================================================
    // Public query API
    // =========================================================================

    /**
     * Returns all active subscriptions for the given base topic name.
     * Returns an empty list if the topic is not in the cache.
     */
    public List<EventSubscription> findByTopic(String topicName) {
        return snapshot.get().byTopicName.getOrDefault(topicName, List.of());
    }

    /** Returns a subscription by its primary key; empty if not found or inactive. */
    public Optional<EventSubscription> findById(String eventSubscriptionId) {
        return Optional.ofNullable(snapshot.get().bySubscriptionId.get(eventSubscriptionId));
    }

    /** Returns all active subscriptions (unmodifiable view). */
    public List<EventSubscription> findAll() {
        return List.copyOf(snapshot.get().bySubscriptionId.values());
    }

    /** Returns all unique active main topic names. */
    public Set<String> activeTopicNames() {
        return Collections.unmodifiableSet(snapshot.get().byTopicName.keySet());
    }

    public int cacheSize() {
        return snapshot.get().bySubscriptionId.size();
    }

    public Instant lastRefreshedAt() {
        return snapshot.get().refreshedAt;
    }

    // =========================================================================
    // Refresh logic
    // =========================================================================

    /**
     * Refreshes the cache from DynamoDB.
     *
     * @return true if the refresh succeeded, false if it failed (old cache is still active)
     */
    public synchronized boolean refresh() {
        log.info("Refreshing subscription cache from DynamoDB");
        try {
            List<EventSubscription> all     = repository.findAll();
            List<EventSubscription> active  = all.stream()
                    .filter(EventSubscription::isFullyActive)
                    .collect(Collectors.toList());

            CacheSnapshot newSnapshot = CacheSnapshot.build(active);
            snapshot.set(newSnapshot);

            log.info("Subscription cache refreshed: total={} active={} topics={}",
                    all.size(), active.size(), newSnapshot.byTopicName.keySet());

            meterRegistry.counter("delivery.cache.refresh.success").increment();
            return true;

        } catch (Exception e) {
            log.error("Failed to refresh subscription cache – retaining last known good snapshot. Error: {}",
                    e.getMessage(), e);
            meterRegistry.counter("delivery.cache.refresh.failure").increment();
            return false;
        }
    }

    // =========================================================================
    // Internal snapshot model
    // =========================================================================

    private record CacheSnapshot(
            Map<String, EventSubscription>       bySubscriptionId,
            Map<String, List<EventSubscription>> byTopicName,
            Map<String, List<EventSubscription>> bySchemaId,
            Instant refreshedAt) {

        static CacheSnapshot empty() {
            return new CacheSnapshot(Map.of(), Map.of(), Map.of(), Instant.now());
        }

        static CacheSnapshot build(List<EventSubscription> subscriptions) {
            Map<String, EventSubscription> byId = new ConcurrentHashMap<>();
            Map<String, List<EventSubscription>> byTopic  = new ConcurrentHashMap<>();
            Map<String, List<EventSubscription>> bySchema = new ConcurrentHashMap<>();

            for (EventSubscription s : subscriptions) {
                byId.put(s.getEventSubscriptionId(), s);
                byTopic .computeIfAbsent(s.getTopicName(),    k -> new ArrayList<>()).add(s);
                bySchema.computeIfAbsent(s.getEventSchemaId(), k -> new ArrayList<>()).add(s);
            }

            return new CacheSnapshot(
                    Collections.unmodifiableMap(byId),
                    Collections.unmodifiableMap(byTopic),
                    Collections.unmodifiableMap(bySchema),
                    Instant.now());
        }
    }
}
