package com.engine.order.service;

import com.engine.order.dto.CreateOrderRequest;
import com.engine.order.dto.OrderResponse;

import java.util.UUID;

public interface OrderService {
    OrderResponse createOrder(CreateOrderRequest request);
    OrderResponse getOrder(UUID id);
}
