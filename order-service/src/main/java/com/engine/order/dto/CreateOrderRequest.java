package com.engine.order.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

public record CreateOrderRequest(
    @NotBlank(message = "userId must not be blank")
    String userId,

    @NotNull(message = "totalAmount must not be null")
    @DecimalMin(value = "0.01", message = "totalAmount must be greater than zero")
    BigDecimal totalAmount,

    @NotBlank(message = "currency must not be blank")
    @Size(min = 3, max = 3, message = "currency must be an ISO 3-letter code")
    String currency
) {}
