package com.engine.order.service;

import com.engine.common.OrderCreatedEvent;
import com.engine.order.domain.Order;
import com.engine.order.domain.OrderStatus;
import com.engine.order.domain.OutboxEvent;
import com.engine.order.dto.CreateOrderRequest;
import com.engine.order.dto.OrderResponse;
import com.engine.order.repository.OrderRepository;
import com.engine.order.repository.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;

@Service
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderServiceImpl(OrderRepository orderRepository, OutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now();

        Order order = new Order(
            orderId,
            request.userId(),
            request.totalAmount(),
            request.currency(),
            OrderStatus.PENDING
        );
        Order savedOrder = orderRepository.save(order);

        OrderCreatedEvent event = new OrderCreatedEvent(
            eventId,
            orderId,
            request.userId(),
            request.totalAmount(),
            request.currency(),
            now
        );

        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OrderCreatedEvent", e);
        }

        OutboxEvent outboxEvent = new OutboxEvent(
            UUID.randomUUID(),
            "ORDER",
            orderId.toString(),
            "ORDER_CREATED",
            payload
        );
        outboxRepository.save(outboxEvent);

        return mapToResponse(savedOrder);
    }

    @Override
    @Transactional(readOnly = true)
    public OrderResponse getOrder(UUID id) {
        Order order = orderRepository.findById(id)
            .orElseThrow(() -> new NoSuchElementException("Order not found: " + id));
        return mapToResponse(order);
    }

    private OrderResponse mapToResponse(Order order) {
        return new OrderResponse(
            order.getId(),
            order.getUserId(),
            order.getTotalAmount(),
            order.getCurrency(),
            order.getStatus(),
            order.getCreatedAt(),
            order.getUpdatedAt()
        );
    }
}
