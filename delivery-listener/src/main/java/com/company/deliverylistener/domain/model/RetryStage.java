package com.company.deliverylistener.domain.model;

/**
 * Represents the current retry stage of an event message in the delivery pipeline.
 *
 * <p>The pipeline progresses: MAIN → R1 → R2 → R3 → DL
 * Delivery is re-attempted at each stage before escalating to the next.
 */
public enum RetryStage {

    /** Original main topic – first delivery attempt. */
    MAIN(0),

    /** First retry, scheduled 3 minutes after the initial failure. */
    R1(1),

    /** Second retry, scheduled 6 minutes after the initial failure. */
    R2(2),

    /** Third and final retry, scheduled 9 minutes after the initial failure. */
    R3(3),

    /** Dead-letter – no further delivery attempts; for alerting and forensics. */
    DL(4);

    private final int order;

    RetryStage(int order) {
        this.order = order;
    }

    public int getOrder() {
        return order;
    }

    /**
     * Returns the next stage in the retry chain.
     * If already at R3 or DL, returns DL.
     */
    public RetryStage next() {
        return switch (this) {
            case MAIN -> R1;
            case R1   -> R2;
            case R2   -> R3;
            case R3, DL -> DL;
        };
    }

    /**
     * Derives the RetryStage from a topic suffix.
     * e.g. ".R1" → R1, ".DL" → DL, anything else → MAIN.
     */
    public static RetryStage fromTopicSuffix(String suffix) {
        if (suffix == null) return MAIN;
        return switch (suffix.trim().toUpperCase()) {
            case ".R1" -> R1;
            case ".R2" -> R2;
            case ".R3" -> R3;
            case ".DL" -> DL;
            default    -> MAIN;
        };
    }

    public boolean isDead() {
        return this == DL;
    }
}
