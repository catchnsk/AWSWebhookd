package com.company.deliverylistener.api.dto;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

/**
 * Summary of delivery metrics for the admin API.
 * Values are sourced from Micrometer counters and the subscription cache.
 */
@Value
@Builder
public class DeliverySummaryResponse {

    int     activeCacheSize;
    Instant cacheLastRefreshedAt;
    int     activeTopicCount;
    String  serviceUpSince;
}
