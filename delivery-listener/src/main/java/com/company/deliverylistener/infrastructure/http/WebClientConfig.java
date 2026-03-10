package com.company.deliverylistener.infrastructure.http;

import com.company.deliverylistener.config.properties.WebhookProperties;
import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Configures the reactive {@link WebClient} used for outbound webhook delivery.
 *
 * <p>Key settings:
 * <ul>
 *   <li>Fixed connection pool via {@code ConnectionProvider} for controlled resource use.</li>
 *   <li>Connect and read timeouts from {@link WebhookProperties}.</li>
 *   <li>No automatic HTTP-level retries – retries are orchestrated by the Kafka retry topology
 *       to avoid duplicate deliveries and to honour the staged delay requirements.</li>
 * </ul>
 */
@Configuration
@RequiredArgsConstructor
public class WebClientConfig {

    private final WebhookProperties webhookProperties;

    @Bean
    public WebClient webhookWebClient() {
        ConnectionProvider provider = ConnectionProvider.builder("webhook-pool")
                .maxConnections(webhookProperties.getMaxConnections())
                .maxIdleTime(webhookProperties.getMaxIdleTime())
                .maxLifeTime(Duration.ofMinutes(10))
                .pendingAcquireTimeout(Duration.ofSeconds(5))
                .evictInBackground(Duration.ofSeconds(120))
                .build();

        HttpClient httpClient = HttpClient.create(provider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                        (int) webhookProperties.getConnectTimeout().toMillis())
                .doOnConnected(conn -> conn.addHandlerLast(
                        new ReadTimeoutHandler(
                                webhookProperties.getReadTimeout().toMillis(),
                                TimeUnit.MILLISECONDS)));

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }
}
