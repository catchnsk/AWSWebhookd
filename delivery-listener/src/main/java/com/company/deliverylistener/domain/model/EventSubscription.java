package com.company.deliverylistener.domain.model;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

/**
 * Immutable domain representation of a row in {@code glbl-event-subscription} DynamoDB table.
 *
 * <p>Mapping assumptions:
 * <ul>
 *   <li>{@code topicName} stores the <em>base</em> topic name (e.g. {@code Microsoft.cardShip.v1.0}).
 *       Retry and DL topic names are derived by appending configured suffixes (.R1, .R2, .R3, .DL).</li>
 *   <li>If {@code maxAttempts} is present in the table it overrides the global default.</li>
 *   <li>{@code targetDispatcher} is modelled but reserved for a future routing phase.</li>
 *   <li>Status values are normalised to uppercase at parse time.</li>
 * </ul>
 */
@Value
@Builder(toBuilder = true)
public class EventSubscription {

    // ---- Primary key ----
    String eventSubscriptionId;   // PK
    String eventSchemaId;         // SK

    // ---- Subscriber identity ----
    String subscriberEntityId;
    String subscriberName;
    String subscriberRefId;

    // ---- Delivery ----
    String subscriberDeliveryUrl;
    AuthType subscriberAuthType;
    String subscriberProxyUrl;    // optional; null means direct delivery

    // ---- Status ----
    SubscriberStatus subscriberStatus;

    // ---- Event metadata ----
    String eventName;
    String eventVersion;
    String eventOverridesSchema;  // optional

    // ---- Topic ----
    String topicName;             // base topic name (suffixes are appended by the service)
    TopicStatus topicStatus;
    String topicStatusMessage;    // human-readable reason for non-active status

    // ---- Auth (OAuth2 client-credentials) ----
    String tokenEndpoint;         // nullable
    String grantType;             // nullable
    String jwksUriEndpoint;       // nullable; for future JWKS validation

    // ---- Retry config (per-subscription overrides) ----
    Integer maxAttempts;          // null → use global default from application.yml
    String  retryWindow;          // ISO-8601 duration string; reserved for future use

    // ---- Future: dispatcher routing ----
    String targetDispatcher;      // reserved for Day-2 multi-dispatcher routing

    // ---- Audit ----
    Instant insertTimestamp;
    String  insertUser;
    Instant updateTimestamp;
    String  updateUser;

    /**
     * Returns true only when both subscriber and topic are active.
     * This is the authoritative predicate used by the cache loader.
     */
    public boolean isFullyActive() {
        return subscriberStatus != null && subscriberStatus.isActive()
            && topicStatus       != null && topicStatus.isActive();
    }

    /**
     * Derives the retry topic name for a given stage.
     * e.g. base="Microsoft.cardShip.v1.0", stage=R1 → "Microsoft.cardShip.v1.0.R1"
     */
    public String retryTopicName(RetryStage stage, String suffix) {
        return topicName + suffix;
    }
}
