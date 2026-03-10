package com.company.deliverylistener.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Top-level application configuration.
 */
@Data
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    /** Human-readable service name used in logs and metrics tags. */
    private String serviceName = "delivery-listener";

    private Aws aws = new Aws();
    private DynamoDb dynamodb = new DynamoDb();

    @Data
    public static class Aws {
        private String region = "us-east-1";
        /** Optional endpoint override for local testing (e.g. LocalStack). */
        private String endpointOverride;
    }

    @Data
    public static class DynamoDb {
        private String tableName = "glbl-event-subscription";
        /** How often the in-memory subscription cache is refreshed (hours). */
        private int refreshIntervalHours = 24;
    }
}
