package com.company.deliverylistener.infrastructure.dynamodb;

import com.company.deliverylistener.config.properties.AppProperties;
import com.company.deliverylistener.domain.model.*;
import com.company.deliverylistener.domain.repository.SubscriptionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * DynamoDB implementation of {@link SubscriptionRepository}.
 *
 * <p>Uses a full table scan because:
 * <ul>
 *   <li>Scans are only performed once every 24 hours (daily cache refresh).</li>
 *   <li>The table is assumed to be of manageable size (tens of thousands of rows at most).</li>
 *   <li>No GSI currently supports the "all active subscriptions" query pattern efficiently.</li>
 * </ul>
 *
 * <p>Future optimisation: add a GSI on {@code subscriberStatus + topicStatus} and
 * switch from Scan to Query to reduce read-unit consumption.
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class DynamoDbSubscriptionRepository implements SubscriptionRepository {

    private final DynamoDbClient dynamoDbClient;
    private final AppProperties  appProperties;

    @Override
    public List<EventSubscription> findAll() {
        String tableName = appProperties.getDynamodb().getTableName();
        log.info("Scanning DynamoDB table '{}' for all subscription records", tableName);

        List<EventSubscription> results    = new ArrayList<>();
        Map<String, AttributeValue> lastKey = null;

        do {
            var requestBuilder = ScanRequest.builder().tableName(tableName);
            if (lastKey != null) {
                requestBuilder.exclusiveStartKey(lastKey);
            }

            ScanResponse response = dynamoDbClient.scan(requestBuilder.build());
            for (var item : response.items()) {
                try {
                    results.add(mapItem(item));
                } catch (Exception e) {
                    log.warn("Skipping malformed DynamoDB item. Error: {}", e.getMessage());
                }
            }
            lastKey = response.lastEvaluatedKey().isEmpty() ? null : response.lastEvaluatedKey();

        } while (lastKey != null);

        log.info("Loaded {} subscription records from DynamoDB", results.size());
        return results;
    }

    // -------------------------------------------------------------------------
    // Mapping helpers
    // -------------------------------------------------------------------------

    private EventSubscription mapItem(Map<String, AttributeValue> item) {
        return EventSubscription.builder()
                // Primary key
                .eventSubscriptionId(str(item, "eventSubscriptionId"))
                .eventSchemaId(str(item, "eventSchemaId"))
                // Subscriber identity
                .subscriberEntityId(str(item, "subscriberEntityId"))
                .subscriberName(str(item, "subscriberName"))
                .subscriberRefId(str(item, "subscriberRefId"))
                // Delivery
                .subscriberDeliveryUrl(str(item, "subscriberDeliveryUrl"))
                .subscriberAuthType(AuthType.fromString(str(item, "subscriberAuthType")))
                .subscriberProxyUrl(str(item, "subscriberProxyUrl"))
                // Status
                .subscriberStatus(SubscriberStatus.fromString(str(item, "subscriberStatus")))
                // Event metadata
                .eventName(str(item, "eventName"))
                .eventVersion(str(item, "eventVersion"))
                .eventOverridesSchema(str(item, "eventOverridesSchema"))
                // Topic
                .topicName(str(item, "topicName"))
                .topicStatus(TopicStatus.fromString(str(item, "topicStatus")))
                .topicStatusMessage(str(item, "topicStatusMessage"))
                // Auth
                .tokenEndpoint(str(item, "tokenEndpoint"))
                .grantType(str(item, "grantType"))
                .jwksUriEndpoint(str(item, "jwksUriEndpoint"))
                // Retry overrides
                .maxAttempts(numInt(item, "maxAttempts"))
                .retryWindow(str(item, "retryWindow"))
                // Future use
                .targetDispatcher(str(item, "targetDispatcher"))
                // Audit
                .insertTimestamp(instant(item, "insertTimestamp"))
                .insertUser(str(item, "insertUser"))
                .updateTimestamp(instant(item, "updateTimestamp"))
                .updateUser(str(item, "updateUser"))
                .build();
    }

    /** Safely extracts a String attribute; returns null if absent or not a String. */
    private static String str(Map<String, AttributeValue> item, String key) {
        return Optional.ofNullable(item.get(key))
                .filter(v -> v.s() != null)
                .map(AttributeValue::s)
                .orElse(null);
    }

    /** Safely extracts an Integer attribute from a NUMBER type attribute. */
    private static Integer numInt(Map<String, AttributeValue> item, String key) {
        return Optional.ofNullable(item.get(key))
                .filter(v -> v.n() != null)
                .map(v -> {
                    try { return Integer.parseInt(v.n()); }
                    catch (NumberFormatException e) { return null; }
                })
                .orElse(null);
    }

    /** Safely extracts an Instant from a STRING attribute in ISO-8601 format. */
    private static Instant instant(Map<String, AttributeValue> item, String key) {
        String raw = str(item, key);
        if (raw == null) return null;
        try { return Instant.parse(raw); }
        catch (Exception e) { return null; }
    }
}
