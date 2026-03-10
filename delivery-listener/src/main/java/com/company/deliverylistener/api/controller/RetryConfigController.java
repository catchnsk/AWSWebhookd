package com.company.deliverylistener.api.controller;

import com.company.deliverylistener.api.dto.RetryConfigResponse;
import com.company.deliverylistener.config.properties.RetryProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * GET /api/retries/config
 * Exposes the active retry delay configuration for operational verification.
 */
@RestController
@RequestMapping("/api/retries")
@RequiredArgsConstructor
public class RetryConfigController {

    private final RetryProperties retryProperties;

    @GetMapping("/config")
    public RetryConfigResponse config() {
        return RetryConfigResponse.from(retryProperties);
    }
}
