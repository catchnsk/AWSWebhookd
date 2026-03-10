package com.company.deliverylistener.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configurable retry delay windows for each retry stage.
 *
 * <p>These act as global defaults; per-subscription {@code maxAttempts} overrides
 * the number of retries when present in DynamoDB.
 *
 * <p>Default staged delays:
 * <pre>
 *   R1 → 3 minutes after initial failure
 *   R2 → 6 minutes after initial failure
 *   R3 → 9 minutes after initial failure
 *   DL → After exhausting R3 (no additional wait)
 * </pre>
 */
@Data
@ConfigurationProperties(prefix = "retry")
public class RetryProperties {

    /** Maximum nack park duration per retry-consumer poll cycle to avoid max.poll.interval breach. */
    private Duration maxNackDuration = Duration.ofSeconds(30);

    private Delay delay = new Delay();

    @Data
    public static class Delay {
        private Duration r1 = Duration.ofMinutes(3);
        private Duration r2 = Duration.ofMinutes(6);
        private Duration r3 = Duration.ofMinutes(9);
        /** Not an additional delay; marks the total window before DL. */
        private Duration dl = Duration.ofMinutes(15);
    }
}
