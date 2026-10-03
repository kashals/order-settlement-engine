package com.engine.common;

import java.time.Instant;
import java.util.UUID;

public record PaymentFailedEvent(
    UUID eventId,
    UUID orderId,
    String reason,
    Instant failedAt
) {}
