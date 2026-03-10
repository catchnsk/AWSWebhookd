package com.company.deliverylistener.api.controller;

import com.company.deliverylistener.application.service.TopicDiscoveryService;
import com.company.deliverylistener.infrastructure.kafka.consumer.MskConsumerManager;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Set;

/**
 * REST endpoints for active Kafka topic information.
 *
 * <p>GET /api/topics/active
 * Returns all currently active topic names (main + retry variants) as seen by this pod.
 */
@RestController
@RequestMapping("/api/topics")
@RequiredArgsConstructor
public class TopicController {

    private final TopicDiscoveryService topicDiscoveryService;
    private final MskConsumerManager    consumerManager;

    @GetMapping("/active")
    public Map<String, Object> activeTopics() {
        return Map.of(
                "mainTopics",   topicDiscoveryService.discoverMainTopics(),
                "retry1Topics", topicDiscoveryService.discoverRetry1Topics(),
                "retry2Topics", topicDiscoveryService.discoverRetry2Topics(),
                "retry3Topics", topicDiscoveryService.discoverRetry3Topics(),
                "dlTopics",     topicDiscoveryService.discoverDlTopics(),
                "activeConsumerStages", consumerManager.activeStages()
        );
    }
}
