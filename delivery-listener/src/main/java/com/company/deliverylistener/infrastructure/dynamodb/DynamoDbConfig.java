package com.company.deliverylistener.infrastructure.dynamodb;

import com.company.deliverylistener.config.properties.AppProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.URI;

/**
 * Configures the AWS DynamoDB SDK v2 client.
 *
 * <p>In EKS, credentials are supplied automatically via IRSA (IAM Roles for Service Accounts).
 * The {@code DefaultCredentialsProvider} chain is used implicitly by the SDK builder.
 *
 * <p>When {@code app.aws.endpointOverride} is set (e.g., for LocalStack in tests),
 * the client points to the override URL instead of the real AWS endpoint.
 */
@Configuration
public class DynamoDbConfig {

    @Bean
    public DynamoDbClient dynamoDbClient(AppProperties properties) {
        var builder = DynamoDbClient.builder()
                .region(Region.of(properties.getAws().getRegion()));

        // Allow endpoint override for local/integration testing
        String endpointOverride = properties.getAws().getEndpointOverride();
        if (endpointOverride != null && !endpointOverride.isBlank()) {
            builder.endpointOverride(URI.create(endpointOverride));
        }

        return builder.build();
    }

    @Bean
    public DynamoDbEnhancedClient dynamoDbEnhancedClient(DynamoDbClient dynamoDbClient) {
        return DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
    }
}
