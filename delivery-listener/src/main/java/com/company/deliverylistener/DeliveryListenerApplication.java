package com.company.deliverylistener;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Delivery Listener Service.
 *
 * <p>This service consumes events from Amazon MSK (Kafka), looks up active
 * subscriber configurations stored in DynamoDB, and delivers event payloads
 * to subscriber webhook URLs. Failed deliveries are routed through a staged
 * retry topology (R1 → R2 → R3 → DL) using separate Kafka topics.
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan("com.company.deliverylistener.config.properties")
public class DeliveryListenerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeliveryListenerApplication.class, args);
    }
}
