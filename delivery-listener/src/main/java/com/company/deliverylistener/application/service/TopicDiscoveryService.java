package com.company.deliverylistener.application.service;

import com.company.deliverylistener.config.properties.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Derives the full set of Kafka topic names (main + retry variants)
 * from the active subscription cache.
 *
 * <p>Topic naming convention (parameterised via {@link KafkaProperties.TopicNaming}):
 * <pre>
 *   Main:  Microsoft.cardShip.v1.0
 *   R1:    Microsoft.cardShip.v1.0.R1
 *   R2:    Microsoft.cardShip.v1.0.R2
 *   R3:    Microsoft.cardShip.v1.0.R3
 *   DL:    Microsoft.cardShip.v1.0.DL
 * </pre>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TopicDiscoveryService {

    private final SubscriptionCacheService cacheService;
    private final KafkaProperties          kafkaProperties;

    /** All active main topic names (no suffix). */
    public Set<String> discoverMainTopics() {
        return cacheService.activeTopicNames();
    }

    /** All R1 retry topic names. */
    public Set<String> discoverRetry1Topics() {
        return appendSuffix(kafkaProperties.getTopicNaming().getRetry1Suffix());
    }

    /** All R2 retry topic names. */
    public Set<String> discoverRetry2Topics() {
        return appendSuffix(kafkaProperties.getTopicNaming().getRetry2Suffix());
    }

    /** All R3 retry topic names. */
    public Set<String> discoverRetry3Topics() {
        return appendSuffix(kafkaProperties.getTopicNaming().getRetry3Suffix());
    }

    /** All DL topic names (used for publishing only, not subscribing). */
    public Set<String> discoverDlTopics() {
        return appendSuffix(kafkaProperties.getTopicNaming().getDlSuffix());
    }

    /** Returns the base topic name for a given fully-qualified topic (strips known suffixes). */
    public String extractBaseTopic(String topic) {
        KafkaProperties.TopicNaming n = kafkaProperties.getTopicNaming();
        for (String suffix : new String[]{
                n.getDlSuffix(), n.getRetry3Suffix(),
                n.getRetry2Suffix(), n.getRetry1Suffix()}) {
            if (topic.endsWith(suffix)) {
                return topic.substring(0, topic.length() - suffix.length());
            }
        }
        return topic;
    }

    // -------------------------------------------------------------------------

    private Set<String> appendSuffix(String suffix) {
        Set<String> result = new LinkedHashSet<>();
        for (String base : cacheService.activeTopicNames()) {
            result.add(base + suffix);
        }
        return result;
    }
}
