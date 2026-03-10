package com.company.deliverylistener.application.service;

import com.company.deliverylistener.config.properties.WebhookProperties;
import com.company.deliverylistener.domain.model.AuthType;
import com.company.deliverylistener.domain.model.EventSubscription;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Obtains and caches outbound authentication tokens for subscriber delivery.
 *
 * <p>Strategy pattern: each {@link AuthType} maps to a resolver.  New auth types
 * (e.g. MTLS, API_KEY) can be added without modifying existing code.
 *
 * <p>Token caching: tokens are cached per {@code tokenEndpoint} with a TTL equal to
 * the {@code expires_in} value minus a configurable buffer
 * ({@link WebhookProperties#getTokenExpiryBuffer()}).  Expired tokens trigger a fresh fetch.
 *
 * <p><strong>Secret handling</strong>: client ID and secret are injected via environment
 * variables or Kubernetes Secrets.  The DynamoDB record stores only the {@code tokenEndpoint}
 * and {@code grantType}; credentials are <em>never</em> stored in DynamoDB.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuthTokenService {

    private final WebClient           webhookWebClient;
    private final WebhookProperties   webhookProperties;

    /** Cached tokens keyed by tokenEndpoint URL. */
    private final Map<String, CachedToken> tokenCache = new ConcurrentHashMap<>();

    /**
     * Returns a bearer token string, or {@code null} if no auth is required.
     *
     * @param subscription the subscriber subscription record
     * @return bearer token value (without "Bearer " prefix), or null for NONE auth
     */
    public String resolveToken(EventSubscription subscription) {
        AuthType authType = subscription.getSubscriberAuthType();
        return switch (authType) {
            case NONE    -> null;
            case CLIENT_CREDENTIALS, BEARER ->
                fetchOrCachedToken(subscription.getTokenEndpoint(),
                                   subscription.getEventSubscriptionId());
            case MTLS    -> {
                log.warn("MTLS auth not yet implemented for subscriptionId={}; " +
                         "proceeding without auth", subscription.getEventSubscriptionId());
                yield null;
            }
            case UNKNOWN -> {
                log.warn("Unknown auth type for subscriptionId={}; proceeding without auth",
                         subscription.getEventSubscriptionId());
                yield null;
            }
        };
    }

    // -------------------------------------------------------------------------

    private String fetchOrCachedToken(String tokenEndpoint, String subscriptionId) {
        if (tokenEndpoint == null || tokenEndpoint.isBlank()) {
            log.warn("Token endpoint is null for subscriptionId={}; skipping auth", subscriptionId);
            return null;
        }

        CachedToken cached = tokenCache.get(tokenEndpoint);
        if (cached != null && !cached.isExpired(webhookProperties.getTokenExpiryBuffer())) {
            return cached.accessToken;
        }

        return refreshToken(tokenEndpoint, subscriptionId);
    }

    private synchronized String refreshToken(String tokenEndpoint, String subscriptionId) {
        // Double-check after acquiring lock
        CachedToken cached = tokenCache.get(tokenEndpoint);
        if (cached != null && !cached.isExpired(webhookProperties.getTokenExpiryBuffer())) {
            return cached.accessToken;
        }

        // Read client credentials from environment (injected via Kubernetes Secrets)
        String clientId     = System.getenv("OAUTH_CLIENT_ID");
        String clientSecret = System.getenv("OAUTH_CLIENT_SECRET");

        if (clientId == null || clientSecret == null) {
            log.error("OAUTH_CLIENT_ID / OAUTH_CLIENT_SECRET not set; " +
                      "cannot fetch token for subscriptionId={}", subscriptionId);
            return null;
        }

        try {
            MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            form.add("grant_type", "client_credentials");
            form.add("client_id", clientId);
            form.add("client_secret", clientSecret);

            @SuppressWarnings("unchecked")
            Map<String, Object> response = webhookWebClient.post()
                    .uri(tokenEndpoint)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .body(BodyInserters.fromFormData(form))
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block(webhookProperties.getReadTimeout());

            if (response == null) {
                log.error("Null token response from endpoint={}", tokenEndpoint);
                return null;
            }

            String token    = (String) response.get("access_token");
            Object expiresIn = response.get("expires_in");
            long ttlSeconds = expiresIn instanceof Number n ? n.longValue() : 3600L;

            tokenCache.put(tokenEndpoint, new CachedToken(token, Instant.now().plusSeconds(ttlSeconds)));
            log.debug("Fetched new bearer token for endpoint={} expiresIn={}s", tokenEndpoint, ttlSeconds);
            return token;

        } catch (Exception e) {
            log.error("Failed to fetch token from endpoint={} subscriptionId={}: {}",
                    tokenEndpoint, subscriptionId, e.getMessage());
            return null;
        }
    }

    // -------------------------------------------------------------------------

    private record CachedToken(String accessToken, Instant expiresAt) {
        boolean isExpired(java.time.Duration buffer) {
            return Instant.now().isAfter(expiresAt.minus(buffer));
        }
    }
}
