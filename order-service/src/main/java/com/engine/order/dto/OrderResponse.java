package com.engine.order.dto;

import com.engine.order.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
    UUID id,
    String userId,
    BigDecimal totalAmount,
    String currency,
    OrderStatus status,
    Instant createdAt,
    Instant updatedAt
) {}
