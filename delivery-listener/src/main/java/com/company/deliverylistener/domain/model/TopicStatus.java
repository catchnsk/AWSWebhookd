package com.company.deliverylistener.domain.model;

/**
 * Status of the MSK topic associated with a subscription.
 * A subscription is only consumed when BOTH subscriberStatus AND topicStatus are ACTIVE.
 */
public enum TopicStatus {

    ACTIVE,
    INACTIVE,
    ERROR,
    UNKNOWN;

    public static TopicStatus fromString(String value) {
        if (value == null || value.isBlank()) {
            return UNKNOWN;
        }
        try {
            return TopicStatus.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    public boolean isActive() {
        return this == ACTIVE;
    }
}
