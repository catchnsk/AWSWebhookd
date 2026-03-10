package com.company.deliverylistener.domain.model;

/**
 * Lifecycle status of a subscriber record in DynamoDB.
 * Values are normalised to uppercase during parsing so that
 * "active", "Active", "ACTIVE" are all treated equivalently.
 */
public enum SubscriberStatus {

    ACTIVE,
    INACTIVE,
    SUSPENDED,
    UNKNOWN;

    public static SubscriberStatus fromString(String value) {
        if (value == null || value.isBlank()) {
            return UNKNOWN;
        }
        try {
            return SubscriberStatus.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    public boolean isActive() {
        return this == ACTIVE;
    }
}
