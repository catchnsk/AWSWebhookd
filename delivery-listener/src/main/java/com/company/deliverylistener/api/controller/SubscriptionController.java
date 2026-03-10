package com.company.deliverylistener.api.controller;

import com.company.deliverylistener.api.dto.SubscriptionResponse;
import com.company.deliverylistener.application.scheduler.SubscriptionRefreshScheduler;
import com.company.deliverylistener.application.service.SubscriptionCacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST endpoints for subscription cache management.
 *
 * <p>All endpoints require ADMIN role (configured in {@code SecurityConfig}).
 */
@Slf4j
@RestController
@RequestMapping("/api/subscriptions")
@RequiredArgsConstructor
public class SubscriptionController {

    private final SubscriptionCacheService       cacheService;
    private final SubscriptionRefreshScheduler   refreshScheduler;

    /**
     * GET /api/subscriptions
     * Returns all active subscriptions from the in-memory cache.
     */
    @GetMapping
    public List<SubscriptionResponse> getAll() {
        return cacheService.findAll().stream()
                .map(SubscriptionResponse::from)
                .toList();
    }

    /**
     * GET /api/subscriptions/{eventSubscriptionId}
     * Returns a single subscription by its primary key.
     */
    @GetMapping("/{eventSubscriptionId}")
    public ResponseEntity<SubscriptionResponse> getById(@PathVariable String eventSubscriptionId) {
        return cacheService.findById(eventSubscriptionId)
                .map(SubscriptionResponse::from)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * POST /api/subscriptions/refresh
     * Forces an immediate cache refresh from DynamoDB and rebuilds Kafka consumers.
     * Returns 200 on success, 500 if the refresh failed (cache retained).
     */
    @PostMapping("/refresh")
    public ResponseEntity<Map<String, Object>> forceRefresh() {
        log.info("Admin-triggered force refresh initiated");
        boolean success = refreshScheduler.forceRefresh();
        Map<String, Object> body = Map.of(
                "success",     success,
                "cacheSize",   cacheService.cacheSize(),
                "refreshedAt", cacheService.lastRefreshedAt().toString(),
                "activeTopics", cacheService.activeTopicNames().size()
        );
        return success
                ? ResponseEntity.ok(body)
                : ResponseEntity.internalServerError().body(body);
    }
}
