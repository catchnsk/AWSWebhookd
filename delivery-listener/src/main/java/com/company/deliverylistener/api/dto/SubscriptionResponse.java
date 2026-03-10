package com.company.deliverylistener.api.dto;

import com.company.deliverylistener.domain.model.AuthType;
import com.company.deliverylistener.domain.model.EventSubscription;
import com.company.deliverylistener.domain.model.SubscriberStatus;
import com.company.deliverylistener.domain.model.TopicStatus;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Value;

/**
 * API response DTO for a single subscription record.
 * Sensitive fields (tokenEndpoint credentials, proxy details) are omitted.
 */
@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SubscriptionResponse {

    String           eventSubscriptionId;
    String           eventSchemaId;
    String           subscriberEntityId;
    String           subscriberName;
    String           subscriberRefId;
    String           subscriberDeliveryUrl; // masked in production; shown for ops use
    AuthType         subscriberAuthType;
    SubscriberStatus subscriberStatus;
    String           eventName;
    String           eventVersion;
    String           topicName;
    TopicStatus      topicStatus;
    String           topicStatusMessage;
    Integer          maxAttempts;

    public static SubscriptionResponse from(EventSubscription s) {
        return SubscriptionResponse.builder()
                .eventSubscriptionId(s.getEventSubscriptionId())
                .eventSchemaId(s.getEventSchemaId())
                .subscriberEntityId(s.getSubscriberEntityId())
                .subscriberName(s.getSubscriberName())
                .subscriberRefId(s.getSubscriberRefId())
                .subscriberDeliveryUrl(maskUrl(s.getSubscriberDeliveryUrl()))
                .subscriberAuthType(s.getSubscriberAuthType())
                .subscriberStatus(s.getSubscriberStatus())
                .eventName(s.getEventName())
                .eventVersion(s.getEventVersion())
                .topicName(s.getTopicName())
                .topicStatus(s.getTopicStatus())
                .topicStatusMessage(s.getTopicStatusMessage())
                .maxAttempts(s.getMaxAttempts())
                .build();
    }

    private static String maskUrl(String url) {
        if (url == null) return null;
        try {
            var uri = java.net.URI.create(url);
            return uri.getScheme() + "://" + uri.getHost() + "/***";
        } catch (Exception e) { return "***"; }
    }
}
