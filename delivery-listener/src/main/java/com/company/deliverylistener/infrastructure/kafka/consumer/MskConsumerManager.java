package com.company.deliverylistener.infrastructure.kafka.consumer;

import com.company.deliverylistener.config.properties.KafkaProperties;
import com.company.deliverylistener.domain.model.EventSubscription;
import com.company.deliverylistener.domain.model.RetryStage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manages the lifecycle of MSK/Kafka consumer containers.
 *
 * <p><strong>Dynamic topic subscription strategy (approach #3 – explicit topic list)</strong>:
 * Rather than subscribing to a topic pattern and filtering at runtime, this manager
 * maintains one {@link ConcurrentMessageListenerContainer} per stage group (MAIN, R1, R2, R3),
 * each subscribing to the exact list of topics derived from the active subscription cache.
 *
 * <p>Trade-offs vs alternatives:
 * <ul>
 *   <li>Pattern matching (approach #1): simpler setup but may accidentally consume topics
 *       outside the active set, wasting resources and requiring in-consumer filtering.</li>
 *   <li>Full rebuild on each refresh (approach #2): clean but causes a brief consumption
 *       gap during cache refresh. Chosen here because the 24-hour refresh cycle makes this
 *       gap negligible and the approach is highly readable.</li>
 * </ul>
 *
 * <p>On each {@link #rebuildConsumers(List)} call (startup + 24h refresh):
 * <ol>
 *   <li>Compute the new full set of topics per stage from the active subscription list.</li>
 *   <li>Stop and remove any old containers whose topic set has changed.</li>
 *   <li>Start new containers for the updated topic sets.</li>
 * </ol>
 *
 * <p>Consumer group IDs (e.g. {@code group-main}, {@code group-r1}) ensure that all EKS
 * pod replicas share the workload via Kafka's standard partition assignment.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MskConsumerManager {

    private final ConcurrentKafkaListenerContainerFactory<String, String> mainListenerContainerFactory;
    private final ConcurrentKafkaListenerContainerFactory<String, String> r1ListenerContainerFactory;
    private final ConcurrentKafkaListenerContainerFactory<String, String> r2ListenerContainerFactory;
    private final ConcurrentKafkaListenerContainerFactory<String, String> r3ListenerContainerFactory;

    private final MessageDispatchListener  mainListener;
    private final RetryConsumerListener    retryConsumerListener;
    private final KafkaProperties          kafkaProperties;

    /** Keyed by stage name ("MAIN", "R1", "R2", "R3") → running container. */
    private final Map<String, ConcurrentMessageListenerContainer<String, String>> activeContainers =
            new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Called at startup and after each cache refresh.
     * Builds topic sets for every stage and (re)creates containers as needed.
     */
    public synchronized void rebuildConsumers(List<EventSubscription> activeSubscriptions) {
        log.info("Rebuilding MSK consumers for {} active subscriptions", activeSubscriptions.size());

        KafkaProperties.TopicNaming naming = kafkaProperties.getTopicNaming();

        Set<String> mainTopics = extractTopics(activeSubscriptions, "");
        Set<String> r1Topics   = extractTopics(activeSubscriptions, naming.getRetry1Suffix());
        Set<String> r2Topics   = extractTopics(activeSubscriptions, naming.getRetry2Suffix());
        Set<String> r3Topics   = extractTopics(activeSubscriptions, naming.getRetry3Suffix());

        replaceContainer("MAIN", mainTopics, mainListenerContainerFactory,
                (record, ack) -> mainListener.onMessage(record, ack));
        replaceContainer("R1", r1Topics, r1ListenerContainerFactory,
                (record, ack) -> retryConsumerListener.onMessage(record, ack, RetryStage.R1));
        replaceContainer("R2", r2Topics, r2ListenerContainerFactory,
                (record, ack) -> retryConsumerListener.onMessage(record, ack, RetryStage.R2));
        replaceContainer("R3", r3Topics, r3ListenerContainerFactory,
                (record, ack) -> retryConsumerListener.onMessage(record, ack, RetryStage.R3));

        log.info("Consumer rebuild complete. Active stages: {}", activeContainers.keySet());
    }

    /** Returns the names of all currently running consumer stages. */
    public Set<String> activeStages() {
        return Collections.unmodifiableSet(activeContainers.keySet());
    }

    @PreDestroy
    public void stopAll() {
        log.info("Stopping all MSK consumer containers (graceful shutdown)");
        activeContainers.values().forEach(c -> {
            try { c.stop(); } catch (Exception e) { log.warn("Error stopping container: {}", e.getMessage()); }
        });
        activeContainers.clear();
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private Set<String> extractTopics(List<EventSubscription> subscriptions, String suffix) {
        return subscriptions.stream()
                .map(s -> s.getTopicName() + suffix)
                .collect(Collectors.toSet());
    }

    @FunctionalInterface
    private interface StageListener {
        void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack);
    }

    /**
     * Stops the current container for {@code stage} (if running) and starts a fresh one
     * for the new {@code topics} set.  Skips if topics are empty.
     */
    private void replaceContainer(
            String stage,
            Set<String> topics,
            ConcurrentKafkaListenerContainerFactory<String, String> factory,
            StageListener listener) {

        ConcurrentMessageListenerContainer<String, String> old = activeContainers.remove(stage);
        if (old != null) {
            log.debug("Stopping old consumer container for stage={}", stage);
            old.stop();
        }

        if (topics.isEmpty()) {
            log.info("No active topics for stage={}; skipping container creation", stage);
            return;
        }

        String[] topicArray = topics.toArray(new String[0]);
        ContainerProperties containerProperties = new ContainerProperties(topicArray);
        containerProperties.setAckMode(
                factory.getContainerProperties().getAckMode());
        containerProperties.setShutdownTimeout(30_000L);
        containerProperties.setMessageListener(
                (AcknowledgingMessageListener<String, String>) listener::onMessage);

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(
                        factory.getConsumerFactory(), containerProperties);
        container.setConcurrency(kafkaProperties.getConsumerConcurrency());
        container.setBeanName("delivery-listener-" + stage.toLowerCase());

        log.info("Starting consumer container for stage={}, topics={}", stage, topics);
        container.start();
        activeContainers.put(stage, container);
    }
}
