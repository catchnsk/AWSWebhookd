package com.company.deliverylistener.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Outbound webhook delivery HTTP client configuration.
 */
@Data
@ConfigurationProperties(prefix = "webhook")
public class WebhookProperties {

    /** TCP connection timeout. */
    private Duration connectTimeout = Duration.ofSeconds(5);

    /** Socket / read timeout for the entire response. */
    private Duration readTimeout = Duration.ofSeconds(30);

    /** Maximum connections in the reactor-netty connection pool. */
    private int maxConnections = 200;

    /** Maximum idle time before a pooled connection is evicted. */
    private Duration maxIdleTime = Duration.ofSeconds(60);

    /**
     * HTTP status codes that are considered retriable (in addition to 5xx and timeouts).
     * 429 (Too Many Requests) is included by default.
     */
    private java.util.List<Integer> retriableStatusCodes = java.util.List.of(429, 503);

    /**
     * 4xx codes that ARE retriable (e.g., 408 Request Timeout).
     * By default 4xx is non-retriable unless listed here.
     */
    private java.util.List<Integer> retriable4xxCodes = java.util.List.of(408, 429);

    /**
     * Token endpoint response clock-skew tolerance.
     * Tokens are considered expired {@code tokenExpiryBuffer} before their actual expiry.
     */
    private Duration tokenExpiryBuffer = Duration.ofSeconds(30);
}
