package com.company.deliverylistener.http;

import com.company.deliverylistener.config.properties.WebhookProperties;
import com.company.deliverylistener.domain.model.DeliveryResult;
import com.company.deliverylistener.infrastructure.http.WebhookDeliveryClient;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.*;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit/integration tests for {@link WebhookDeliveryClient} using WireMock.
 */
@DisplayName("WebhookDeliveryClient tests")
class WebhookDeliveryClientTest {

    private static WireMockServer wireMockServer;
    private WebhookDeliveryClient deliveryClient;

    @BeforeAll
    static void startWireMock() {
        wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMockServer.start();
        WireMock.configureFor("localhost", wireMockServer.port());
    }

    @AfterAll
    static void stopWireMock() {
        wireMockServer.stop();
    }

    @BeforeEach
    void setUp() {
        wireMockServer.resetAll();

        WebhookProperties props = new WebhookProperties();
        props.setConnectTimeout(Duration.ofSeconds(2));
        props.setReadTimeout(Duration.ofSeconds(5));
        props.setMaxConnections(10);
        props.setMaxIdleTime(Duration.ofSeconds(30));
        props.setRetriable4xxCodes(List.of(408, 429));

        // Simple WebClient for tests (no reactor-netty timeout handler in unit test)
        WebClient webClient = WebClient.builder().build();
        deliveryClient = new WebhookDeliveryClient(webClient, props);
    }

    @Test
    @DisplayName("200 OK response → success result")
    void deliver_200_returnsSuccess() {
        stubFor(post(urlEqualTo("/webhook"))
                .willReturn(aResponse().withStatus(200).withBody("OK")));

        DeliveryResult result = deliveryClient.deliver(
                wireMockUrl("/webhook"), "{}", "cardActivation", "1.0",
                "sub-1", "corr-1", null);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getHttpStatus()).isEqualTo(200);
    }

    @Test
    @DisplayName("503 Service Unavailable → retriable failure")
    void deliver_503_returnsRetriableFailure() {
        stubFor(post(urlEqualTo("/webhook"))
                .willReturn(aResponse().withStatus(503).withBody("Service Unavailable")));

        DeliveryResult result = deliveryClient.deliver(
                wireMockUrl("/webhook"), "{}", "cardActivation", "1.0",
                "sub-1", "corr-1", null);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.isRetriable()).isTrue();
        assertThat(result.getHttpStatus()).isEqualTo(503);
    }

    @Test
    @DisplayName("429 Too Many Requests → retriable failure")
    void deliver_429_returnsRetriableFailure() {
        stubFor(post(urlEqualTo("/webhook"))
                .willReturn(aResponse().withStatus(429)));

        DeliveryResult result = deliveryClient.deliver(
                wireMockUrl("/webhook"), "{}", "cardActivation", "1.0",
                "sub-1", "corr-1", null);

        assertThat(result.isRetriable()).isTrue();
        assertThat(result.getHttpStatus()).isEqualTo(429);
    }

    @Test
    @DisplayName("404 Not Found → non-retriable failure")
    void deliver_404_returnsNonRetriableFailure() {
        stubFor(post(urlEqualTo("/webhook"))
                .willReturn(aResponse().withStatus(404)));

        DeliveryResult result = deliveryClient.deliver(
                wireMockUrl("/webhook"), "{}", "cardActivation", "1.0",
                "sub-1", "corr-1", null);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.isRetriable()).isFalse();
        assertThat(result.getHttpStatus()).isEqualTo(404);
    }

    @Test
    @DisplayName("201 Created → success result")
    void deliver_201_returnsSuccess() {
        stubFor(post(urlEqualTo("/webhook"))
                .willReturn(aResponse().withStatus(201)));

        DeliveryResult result = deliveryClient.deliver(
                wireMockUrl("/webhook"), "{}", "cardActivation", "1.0",
                "sub-1", "corr-1", null);

        assertThat(result.isSuccess()).isTrue();
    }

    @Test
    @DisplayName("Bearer token is sent in Authorization header")
    void deliver_withBearerToken_sendsAuthHeader() {
        stubFor(post(urlEqualTo("/webhook"))
                .withHeader("Authorization", equalTo("Bearer test-token"))
                .willReturn(aResponse().withStatus(200)));

        DeliveryResult result = deliveryClient.deliver(
                wireMockUrl("/webhook"), "{}", "cardActivation", "1.0",
                "sub-1", "corr-1", "test-token");

        assertThat(result.isSuccess()).isTrue();
        verify(postRequestedFor(urlEqualTo("/webhook"))
                .withHeader("Authorization", equalTo("Bearer test-token")));
    }

    @Test
    @DisplayName("Correlation ID is passed as X-Correlation-Id header")
    void deliver_sendsCorrelationIdHeader() {
        stubFor(post(anyUrl()).willReturn(aResponse().withStatus(200)));

        deliveryClient.deliver(wireMockUrl("/webhook"), "{}", "cardActivation",
                "1.0", "sub-1", "my-corr-id-123", null);

        verify(postRequestedFor(anyUrl())
                .withHeader("X-Correlation-Id", equalTo("my-corr-id-123")));
    }

    @Test
    @DisplayName("Invalid URL (contains spaces – not a valid URI) → non-retriable failure")
    void deliver_invalidUrl_returnsNonRetriableFailure() {
        // A URL with spaces causes URI.create() to throw IllegalArgumentException → non-retriable
        DeliveryResult result = deliveryClient.deliver(
                "http://invalid url with spaces/webhook", "{}", "cardActivation",
                "1.0", "sub-1", "corr-1", null);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.isRetriable()).isFalse();
    }

    // -------------------------------------------------------------------------

    private String wireMockUrl(String path) {
        return "http://localhost:" + wireMockServer.port() + path;
    }
}
