package com.company.deliverylistener.infrastructure.http;

import com.company.deliverylistener.config.properties.WebhookProperties;
import com.company.deliverylistener.domain.model.DeliveryResult;
import com.company.deliverylistener.infrastructure.kafka.headers.RetryHeaders;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Mono;

import java.net.ProxySelector;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;

/**
 * Performs outbound HTTP POST delivery of event payloads to subscriber webhook URLs.
 *
 * <p>Error classification:
 * <ul>
 *   <li><strong>Retriable</strong>: network timeout, connection refused, 5xx, 429, 408, 503.</li>
 *   <li><strong>Non-retriable</strong>: malformed URL, 4xx (except configurable list),
 *       message serialisation errors.</li>
 * </ul>
 *
 * <p>Sensitive data (Authorization header values, bearer tokens) are masked in logs.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WebhookDeliveryClient {

    private final WebClient         webhookWebClient;
    private final WebhookProperties webhookProperties;

    /**
     * Delivers an event payload to the given URL.
     *
     * @param deliveryUrl      subscriber's webhook endpoint
     * @param payload          raw JSON payload to POST
     * @param eventName        used in {@code X-Event-Name} header
     * @param eventVersion     used in {@code X-Event-Version} header
     * @param subscriptionId   used in {@code X-Subscription-Id} header
     * @param correlationId    propagated correlation ID
     * @param bearerToken      optional Bearer token; null means no Authorization header
     * @return delivery result (blocking call – intentional for Kafka listener thread)
     */
    public DeliveryResult deliver(String deliveryUrl,
                                  String payload,
                                  String eventName,
                                  String eventVersion,
                                  String subscriptionId,
                                  String correlationId,
                                  String bearerToken) {
        Instant start = Instant.now();

        try {
            URI uri = URI.create(deliveryUrl);

            var requestSpec = webhookWebClient.post()
                    .uri(uri)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header(RetryHeaders.HTTP_CORRELATION_ID,  correlationId)
                    .header(RetryHeaders.HTTP_EVENT_NAME,      eventName)
                    .header(RetryHeaders.HTTP_EVENT_VERSION,   eventVersion)
                    .header(RetryHeaders.HTTP_SUBSCRIPTION_ID, subscriptionId);

            if (bearerToken != null) {
                requestSpec = requestSpec.header(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken);
            }

            ClientResponse response = requestSpec
                    .bodyValue(payload)
                    .exchange()
                    .block(webhookProperties.getReadTimeout().plusSeconds(5));

            if (response == null) {
                return DeliveryResult.retriableFailure(-1, "No response (null)",
                        elapsed(start));
            }

            int statusCode = response.statusCode().value();
            // Drain the response body to release the connection
            response.releaseBody().block(Duration.ofSeconds(5));

            log.info("Webhook delivery url={} status={} correlationId={}",
                    maskUrl(deliveryUrl), statusCode, correlationId);

            if (HttpStatus.valueOf(statusCode).is2xxSuccessful()) {
                return DeliveryResult.success(statusCode, elapsed(start));
            }

            return classifyHttpError(statusCode, "HTTP " + statusCode, elapsed(start));

        } catch (WebClientRequestException e) {
            // Covers connection refused, timeout, DNS failure, etc.
            log.warn("Network error delivering to url={} correlationId={}: {}",
                    maskUrl(deliveryUrl), correlationId, e.getMessage());
            return DeliveryResult.retriableFailure(-1, "Network error: " + e.getMessage(), elapsed(start));

        } catch (IllegalArgumentException e) {
            // Malformed URL
            log.error("Invalid subscriber delivery URL='{}' correlationId={}: {}",
                    deliveryUrl, correlationId, e.getMessage());
            return DeliveryResult.nonRetriableFailure(-1, "Invalid URL: " + e.getMessage(), elapsed(start));

        } catch (Exception e) {
            log.error("Unexpected error delivering to url={} correlationId={}",
                    maskUrl(deliveryUrl), correlationId, e);
            return DeliveryResult.retriableFailure(-1, "Unexpected: " + e.getMessage(), elapsed(start));
        }
    }

    // -------------------------------------------------------------------------

    private DeliveryResult classifyHttpError(int statusCode, String reason, Duration elapsed) {
        // 5xx is always retriable
        if (statusCode >= 500) {
            return DeliveryResult.retriableFailure(statusCode, reason, elapsed);
        }
        // Specific 4xx codes configured as retriable (e.g. 429, 408)
        if (webhookProperties.getRetriable4xxCodes().contains(statusCode)) {
            return DeliveryResult.retriableFailure(statusCode, reason, elapsed);
        }
        // All other 4xx are non-retriable
        return DeliveryResult.nonRetriableFailure(statusCode, reason, elapsed);
    }

    private static Duration elapsed(Instant start) {
        return Duration.between(start, Instant.now());
    }

    /** Masks query parameters and path after the hostname to avoid logging sensitive data. */
    private static String maskUrl(String url) {
        if (url == null) return "null";
        try {
            URI uri = URI.create(url);
            return uri.getScheme() + "://" + uri.getHost() + "/***";
        } catch (Exception e) {
            return "***";
        }
    }
}
