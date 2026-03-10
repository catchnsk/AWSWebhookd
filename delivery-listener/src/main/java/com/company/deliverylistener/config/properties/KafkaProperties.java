package com.company.deliverylistener.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Kafka / MSK consumer and topic naming configuration.
 *
 * <p>Topic naming convention: given a base topic name stored in DynamoDB,
 * retry and DL topics are derived by appending the configured suffixes.
 * <pre>
 *   Main:  Microsoft.cardShip.v1.0
 *   R1:    Microsoft.cardShip.v1.0.R1
 *   R2:    Microsoft.cardShip.v1.0.R2
 *   R3:    Microsoft.cardShip.v1.0.R3
 *   DL:    Microsoft.cardShip.v1.0.DL
 * </pre>
 */
@Data
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {

    private String bootstrapServers = "localhost:9092";
    private String groupId = "delivery-listener-group";
    /** Number of concurrent consumer threads per listener container. */
    private int consumerConcurrency = 3;
    /** Max poll records per consumer poll cycle. */
    private int maxPollRecords = 50;
    /** Max poll interval (ms) – critical for retry consumers that may park temporarily. */
    private int maxPollIntervalMs = 600_000;

    /** Security protocol: PLAINTEXT | SSL | SASL_SSL (MSK uses SASL_SSL in production). */
    private String securityProtocol = "SASL_SSL";
    private String saslMechanism = "AWS_MSK_IAM";
    private String saslJaasConfig =
            "software.amazon.msk.auth.iam.IAMLoginModule required;";
    private String saslCallbackHandlerClass =
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler";

    private TopicNaming topicNaming = new TopicNaming();

    @Data
    public static class TopicNaming {
        private String retry1Suffix = ".R1";
        private String retry2Suffix = ".R2";
        private String retry3Suffix = ".R3";
        private String dlSuffix     = ".DL";
    }
}
