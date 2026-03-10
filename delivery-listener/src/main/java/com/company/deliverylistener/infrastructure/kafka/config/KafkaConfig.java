package com.company.deliverylistener.infrastructure.kafka.config;

import com.company.deliverylistener.config.properties.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Spring Kafka configuration – consumer and producer factories.
 *
 * <p>Design decisions:
 * <ul>
 *   <li><strong>Manual acknowledgment</strong>: {@code AckMode.MANUAL_IMMEDIATE} ensures that
 *       offsets are committed only after successful delivery or explicit routing to a retry topic.
 *       This prevents message loss on pod restart.</li>
 *   <li><strong>Idempotent producer</strong>: {@code enable.idempotence=true} is set so that
 *       retry-topic publishes are not duplicated during transient Kafka broker issues.</li>
 *   <li><strong>Dynamic consumers</strong>: Actual topic subscriptions are managed programmatically
 *       by {@code MskConsumerManager} using {@code ConcurrentMessageListenerContainer} instances
 *       created from the factories defined here.  This is approach #3 (explicit topic list from
 *       cache) because it avoids broad pattern matching and gives precise control over which
 *       topics each pod consumes.</li>
 *   <li><strong>Consumer groups</strong>: Main and each retry stage use distinct group IDs
 *       (e.g. {@code group-main}, {@code group-r1}) so that EKS horizontal scaling and
 *       partition assignment are independent per stage.</li>
 * </ul>
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    // =========================================================================
    // Consumer factories
    // =========================================================================

    @Bean
    public ConsumerFactory<String, String> mainConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(
                kafkaProperties.getGroupId() + "-main"
        ));
    }

    @Bean
    public ConsumerFactory<String, String> r1ConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(
                kafkaProperties.getGroupId() + "-r1"
        ));
    }

    @Bean
    public ConsumerFactory<String, String> r2ConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(
                kafkaProperties.getGroupId() + "-r2"
        ));
    }

    @Bean
    public ConsumerFactory<String, String> r3ConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(
                kafkaProperties.getGroupId() + "-r3"
        ));
    }

    // =========================================================================
    // Listener container factories
    // =========================================================================

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> mainListenerContainerFactory() {
        return buildFactory(mainConsumerFactory());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> r1ListenerContainerFactory() {
        return buildFactory(r1ConsumerFactory());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> r2ListenerContainerFactory() {
        return buildFactory(r2ConsumerFactory());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> r3ListenerContainerFactory() {
        return buildFactory(r3ConsumerFactory());
    }

    // =========================================================================
    // Producer factory (used for retry/DL publishing)
    // =========================================================================

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Idempotent producer avoids duplicate retry-topic publishes during transient failures
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.RETRIES_CONFIG, 3);
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        addSaslConfig(configs);
        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

    private Map<String, Object> consumerConfigs(String groupId) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Manual offset commit – we commit only after successful processing or retry routing
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaProperties.getMaxPollRecords());
        // Generous poll interval to support retry consumers that may park briefly
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaProperties.getMaxPollIntervalMs());
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // Fetch slightly larger batches for throughput
        configs.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        configs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        addSaslConfig(configs);
        return configs;
    }

    private ConcurrentKafkaListenerContainerFactory<String, String> buildFactory(
            ConsumerFactory<String, String> consumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(kafkaProperties.getConsumerConcurrency());
        // MANUAL_IMMEDIATE: Acknowledgment.acknowledge() commits the offset immediately
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Graceful shutdown – wait for current record to finish processing
        factory.getContainerProperties().setShutdownTimeout(30_000L);
        return factory;
    }

    /**
     * Adds SASL/IAM configuration for MSK.
     * When running locally or in tests, {@code securityProtocol=PLAINTEXT} bypasses this.
     */
    private void addSaslConfig(Map<String, Object> configs) {
        String protocol = kafkaProperties.getSecurityProtocol();
        configs.put("security.protocol", protocol);
        if ("SASL_SSL".equalsIgnoreCase(protocol)) {
            configs.put("sasl.mechanism",                         kafkaProperties.getSaslMechanism());
            configs.put("sasl.jaas.config",                       kafkaProperties.getSaslJaasConfig());
            configs.put("sasl.client.callback.handler.class",     kafkaProperties.getSaslCallbackHandlerClass());
        }
    }
}
