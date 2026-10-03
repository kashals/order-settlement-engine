package com.engine.common;

import java.time.Instant;
import java.util.UUID;

public record PaymentCompletedEvent(
    UUID eventId,
    UUID paymentId,
    UUID orderId,
    String transactionReference,
    Instant completedAt
) {}
