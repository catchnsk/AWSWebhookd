package com.company.deliverylistener.api.controller;

import com.company.deliverylistener.api.dto.DeliverySummaryResponse;
import com.company.deliverylistener.application.service.SubscriptionCacheService;
import com.company.deliverylistener.application.service.TopicDiscoveryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * GET /api/metrics/delivery-summary
 * Provides a high-level operational summary for dashboards and runbook checks.
 * Full Prometheus metrics are available at /actuator/prometheus.
 */
@RestController
@RequestMapping("/api/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final SubscriptionCacheService cacheService;
    private final TopicDiscoveryService    topicDiscoveryService;

    @GetMapping("/delivery-summary")
    public DeliverySummaryResponse summary() {
        long uptimeMs = ManagementFactory.getRuntimeMXBean().getUptime();
        Instant upSince = Instant.now().minus(uptimeMs, ChronoUnit.MILLIS);

        return DeliverySummaryResponse.builder()
                .activeCacheSize(cacheService.cacheSize())
                .cacheLastRefreshedAt(cacheService.lastRefreshedAt())
                .activeTopicCount(topicDiscoveryService.discoverMainTopics().size())
                .serviceUpSince(upSince.toString())
                .build();
    }
}
