package com.engine.order.consumer;

import com.engine.common.PaymentCompletedEvent;
import com.engine.common.PaymentFailedEvent;
import com.engine.order.domain.Order;
import com.engine.order.domain.OrderStatus;
import com.engine.order.repository.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Component
public class PaymentResultConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentResultConsumer.class);

    private final OrderRepository orderRepository;
    private final ObjectMapper objectMapper;

    public PaymentResultConsumer(OrderRepository orderRepository, ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "payment.events", groupId = "order-service-group")
    @Transactional
    public void consume(String message) {
        log.info("Received message from payment.events: {}", message);

        JsonNode root;
        try {
            root = objectMapper.readTree(message);
        } catch (JsonProcessingException e) {
            log.error("Failed to parse payment event payload: {}", message, e);
            return;
        }

        if (root.has("paymentId")) {
            try {
                PaymentCompletedEvent event = objectMapper.treeToValue(root, PaymentCompletedEvent.class);
                handleCompleted(event);
            } catch (JsonProcessingException e) {
                log.error("Failed to deserialize PaymentCompletedEvent: {}", message, e);
            }
        } else if (root.has("reason")) {
            try {
                PaymentFailedEvent event = objectMapper.treeToValue(root, PaymentFailedEvent.class);
                handleFailed(event);
            } catch (JsonProcessingException e) {
                log.error("Failed to deserialize PaymentFailedEvent: {}", message, e);
            }
        }
    }

    private void handleCompleted(PaymentCompletedEvent event) {
        Optional<Order> orderOpt = orderRepository.findById(event.orderId());
        if (orderOpt.isEmpty()) {
            log.warn("Order not found for orderId={}", event.orderId());
            return;
        }

        Order order = orderOpt.get();

        // deduplication check
        if (order.getStatus() == OrderStatus.CONFIRMED || order.getStatus() == OrderStatus.CANCELLED) {
            log.warn("Order {} already in terminal state {}, skipping", order.getId(), order.getStatus());
            return;
        }

        order.setStatus(OrderStatus.CONFIRMED);
        orderRepository.save(order);
        log.info("Order {} transitioned to CONFIRMED", order.getId());
    }

    private void handleFailed(PaymentFailedEvent event) {
        Optional<Order> orderOpt = orderRepository.findById(event.orderId());
        if (orderOpt.isEmpty()) {
            log.warn("Order not found for orderId={}", event.orderId());
            return;
        }

        Order order = orderOpt.get();

        // deduplication check
        if (order.getStatus() == OrderStatus.CONFIRMED || order.getStatus() == OrderStatus.CANCELLED) {
            log.warn("Order {} already in terminal state {}, skipping", order.getId(), order.getStatus());
            return;
        }

        order.setStatus(OrderStatus.CANCELLED);
        orderRepository.save(order);
        log.info("Order {} transitioned to CANCELLED", order.getId());
    }
}
