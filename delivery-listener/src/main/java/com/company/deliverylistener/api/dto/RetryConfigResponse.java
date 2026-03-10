package com.company.deliverylistener.api.dto;

import com.company.deliverylistener.config.properties.RetryProperties;
import lombok.Builder;
import lombok.Value;

import java.time.Duration;

/**
 * Exposes current retry configuration via the admin API.
 * Useful for validating deployed configuration without pod shell access.
 */
@Value
@Builder
public class RetryConfigResponse {

    String r1Delay;
    String r2Delay;
    String r3Delay;
    String dlDelay;
    String maxNackDuration;
    int    defaultMaxAttempts;

    public static RetryConfigResponse from(RetryProperties props) {
        return RetryConfigResponse.builder()
                .r1Delay(formatDuration(props.getDelay().getR1()))
                .r2Delay(formatDuration(props.getDelay().getR2()))
                .r3Delay(formatDuration(props.getDelay().getR3()))
                .dlDelay(formatDuration(props.getDelay().getDl()))
                .maxNackDuration(formatDuration(props.getMaxNackDuration()))
                .defaultMaxAttempts(3)
                .build();
    }

    private static String formatDuration(Duration d) {
        return d != null ? d.toString() : "PT0S";
    }
}
