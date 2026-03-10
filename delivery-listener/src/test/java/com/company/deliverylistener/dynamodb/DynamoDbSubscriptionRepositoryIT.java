package com.company.deliverylistener.dynamodb;

import com.company.deliverylistener.config.properties.AppProperties;
import com.company.deliverylistener.domain.model.SubscriberStatus;
import com.company.deliverylistener.domain.model.TopicStatus;
import com.company.deliverylistener.infrastructure.dynamodb.DynamoDbSubscriptionRepository;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;

/**
 * Integration test for {@link DynamoDbSubscriptionRepository} using LocalStack.
 *
 * <p>Creates the {@code glbl-event-subscription} table in LocalStack and verifies
 * the full scan and mapping behaviour.
 */
@Testcontainers
@DisplayName("DynamoDbSubscriptionRepository integration tests")
class DynamoDbSubscriptionRepositoryIT {

    @Container
    static LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.4"))
            .withServices(DYNAMODB);

    private DynamoDbClient dynamoDbClient;
    private DynamoDbSubscriptionRepository repository;

    static final String TABLE_NAME = "glbl-event-subscription";

    @BeforeEach
    void setUp() {
        dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(localstack.getEndpointOverride(DYNAMODB))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .region(Region.US_EAST_1)
                .build();

        createTable();

        AppProperties appProperties = new AppProperties();
        appProperties.getAws().setRegion("us-east-1");
        appProperties.getAws().setEndpointOverride(
                localstack.getEndpointOverride(DYNAMODB).toString());
        appProperties.getDynamodb().setTableName(TABLE_NAME);

        repository = new DynamoDbSubscriptionRepository(dynamoDbClient, appProperties);
    }

    @AfterEach
    void tearDown() {
        try {
            dynamoDbClient.deleteTable(DeleteTableRequest.builder()
                    .tableName(TABLE_NAME).build());
        } catch (Exception ignored) {}
    }

    @Test
    @DisplayName("findAll returns all items including inactive")
    void findAll_returnsAllItems() {
        putItem("sub-1", "schema-1", "ACTIVE",  "ACTIVE",  "Microsoft.cardShip.v1.0");
        putItem("sub-2", "schema-2", "INACTIVE", "ACTIVE", "Microsoft.cardShip.v1.0");
        putItem("sub-3", "schema-3", "ACTIVE",  "INACTIVE","Uscb.cardTracker.v1.0");

        var results = repository.findAll();

        assertThat(results).hasSize(3);
    }

    @Test
    @DisplayName("findAll maps subscriberStatus correctly")
    void findAll_mapsSubscriberStatus() {
        putItem("sub-active",   "schema-1", "ACTIVE",   "ACTIVE", "topicA");
        putItem("sub-inactive", "schema-2", "inactive", "ACTIVE", "topicA"); // lowercase normalised

        var results = repository.findAll();
        var active   = results.stream()
                .filter(s -> s.getSubscriberStatus() == SubscriberStatus.ACTIVE).toList();
        var inactive = results.stream()
                .filter(s -> s.getSubscriberStatus() == SubscriberStatus.INACTIVE).toList();

        assertThat(active).hasSize(1);
        assertThat(inactive).hasSize(1);
    }

    @Test
    @DisplayName("findAll maps topicStatus correctly")
    void findAll_mapsTopicStatus() {
        putItem("sub-1", "schema-1", "ACTIVE", "ACTIVE",   "topicA");
        putItem("sub-2", "schema-2", "ACTIVE", "inactive", "topicB"); // lowercase

        var results = repository.findAll();
        var activeTopic = results.stream()
                .filter(s -> s.getTopicStatus() == TopicStatus.ACTIVE).toList();

        assertThat(activeTopic).hasSize(1);
    }

    @Test
    @DisplayName("findAll handles missing optional columns gracefully")
    void findAll_missingOptionalColumns_noException() {
        // Minimal record without optional fields
        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(TABLE_NAME)
                .item(Map.of(
                        "eventSubscriptionId", AttributeValue.fromS("sub-minimal"),
                        "eventSchemaId",       AttributeValue.fromS("schema-minimal"),
                        "subscriberStatus",    AttributeValue.fromS("ACTIVE"),
                        "topicStatus",         AttributeValue.fromS("ACTIVE"),
                        "topicName",           AttributeValue.fromS("topicMin")
                )).build());

        var results = repository.findAll();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).getTokenEndpoint()).isNull();
        assertThat(results.get(0).getMaxAttempts()).isNull();
    }

    @Test
    @DisplayName("findAll handles empty table")
    void findAll_emptyTable_returnsEmptyList() {
        var results = repository.findAll();
        assertThat(results).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void createTable() {
        dynamoDbClient.createTable(CreateTableRequest.builder()
                .tableName(TABLE_NAME)
                .attributeDefinitions(
                        AttributeDefinition.builder()
                                .attributeName("eventSubscriptionId")
                                .attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder()
                                .attributeName("eventSchemaId")
                                .attributeType(ScalarAttributeType.S).build())
                .keySchema(
                        KeySchemaElement.builder()
                                .attributeName("eventSubscriptionId")
                                .keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder()
                                .attributeName("eventSchemaId")
                                .keyType(KeyType.RANGE).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
    }

    private void putItem(String subId, String schemaId,
                         String subStatus, String topicStatus, String topicName) {
        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(TABLE_NAME)
                .item(Map.of(
                        "eventSubscriptionId", AttributeValue.fromS(subId),
                        "eventSchemaId",       AttributeValue.fromS(schemaId),
                        "subscriberStatus",    AttributeValue.fromS(subStatus),
                        "topicStatus",         AttributeValue.fromS(topicStatus),
                        "topicName",           AttributeValue.fromS(topicName),
                        "subscriberDeliveryUrl", AttributeValue.fromS("https://example.com/wh"),
                        "subscriberAuthType",  AttributeValue.fromS("NONE"),
                        "subscriberEntityId",  AttributeValue.fromS("entity-1"),
                        "eventName",           AttributeValue.fromS("cardActivation"),
                        "eventVersion",        AttributeValue.fromS("1.0")
                )).build());
    }
}
