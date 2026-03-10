package com.company.deliverylistener.domain.repository;

import com.company.deliverylistener.domain.model.EventSubscription;

import java.util.List;

/**
 * Port (interface) for loading subscription metadata from the backing store.
 * The DynamoDB implementation lives in the infrastructure layer.
 */
public interface SubscriptionRepository {

    /**
     * Loads <em>all</em> subscription records from the backing store.
     * Filtering by active status is done by the cache service, not here,
     * to keep the repository simple and testable.
     *
     * @return full list of all subscriptions; never null, possibly empty
     */
    List<EventSubscription> findAll();
}
