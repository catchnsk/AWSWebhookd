package com.company.deliverylistener.application.scheduler;

import com.company.deliverylistener.application.service.SubscriptionCacheService;
import com.company.deliverylistener.domain.model.EventSubscription;
import com.company.deliverylistener.infrastructure.kafka.consumer.MskConsumerManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Drives the 24-hour subscription cache refresh cycle.
 *
 * <p>On each scheduled run:
 * <ol>
 *   <li>Refresh the {@link SubscriptionCacheService} from DynamoDB.</li>
 *   <li>If successful, rebuild the MSK consumer containers via {@link MskConsumerManager}
 *       so that any newly active or deactivated topics are picked up.</li>
 *   <li>If the refresh fails, the existing cache and consumers remain active.</li>
 * </ol>
 *
 * <p>The schedule is driven by the fixed-delay parameter (in milliseconds) computed
 * from {@code app.dynamodb.refreshIntervalHours}.  A separate property
 * {@code scheduling.subscription-refresh.cron} can override with a cron expression
 * for maintenance-window alignment.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SubscriptionRefreshScheduler {

    private final SubscriptionCacheService cacheService;
    private final MskConsumerManager       consumerManager;

    /**
     * Scheduled refresh.  The fixed-rate is 24 hours expressed in milliseconds.
     * Override with {@code scheduling.subscription-refresh.fixedRateMs} in application.yml.
     */
    @Scheduled(fixedRateString = "${scheduling.subscription-refresh.fixedRateMs:86400000}",
               initialDelayString = "${scheduling.subscription-refresh.initialDelayMs:0}")
    public void scheduledRefresh() {
        log.info("Scheduled subscription cache refresh starting");
        doRefresh();
    }

    /**
     * Force-refresh triggered by the admin API.
     * Returns true if the refresh succeeded.
     */
    public boolean forceRefresh() {
        log.info("Force refresh of subscription cache requested via admin API");
        return doRefresh();
    }

    // -------------------------------------------------------------------------

    private boolean doRefresh() {
        boolean success = cacheService.refresh();
        if (success) {
            List<EventSubscription> active = cacheService.findAll();
            log.info("Rebuilding MSK consumers after successful cache refresh: {} active subscriptions",
                    active.size());
            consumerManager.rebuildConsumers(active);
        } else {
            log.warn("Cache refresh failed – MSK consumers NOT rebuilt; continuing with stale cache");
        }
        return success;
    }
}
