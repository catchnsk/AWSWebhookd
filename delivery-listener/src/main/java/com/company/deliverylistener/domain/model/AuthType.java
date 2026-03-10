package com.company.deliverylistener.domain.model;

/**
 * Supported outbound authentication types for webhook delivery.
 *
 * <p>Additional types (e.g., MTLS, API_KEY, HMAC) can be added here
 * as the strategy pattern in {@code AuthTokenService} handles each type.
 */
public enum AuthType {

    /** No authentication header is added to the outbound request. */
    NONE,

    /**
     * OAuth 2.0 Client Credentials grant.
     * Token is fetched from {@code tokenEndpoint} and cached until expiry.
     */
    CLIENT_CREDENTIALS,

    /**
     * Static or pre-obtained bearer token.
     * Future: may read from Secrets Manager rather than the table directly.
     */
    BEARER,

    /** Placeholder for future mTLS support. */
    MTLS,

    /** Unknown / unsupported – treated as NONE with a warning logged. */
    UNKNOWN;

    /**
     * Parses a raw string value from DynamoDB, normalising case differences.
     * Falls back to {@code UNKNOWN} rather than throwing.
     */
    public static AuthType fromString(String value) {
        if (value == null || value.isBlank()) {
            return NONE;
        }
        try {
            return AuthType.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
