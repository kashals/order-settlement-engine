package com.engine.order.consumer;

import com.engine.common.PaymentCompletedEvent;
import com.engine.common.PaymentFailedEvent;
import com.engine.order.domain.Order;
import com.engine.order.domain.OrderStatus;
import com.engine.order.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentResultConsumerTest {

    @Mock
    private OrderRepository orderRepository;

    private ObjectMapper objectMapper;
    private PaymentResultConsumer consumer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        consumer = new PaymentResultConsumer(orderRepository, objectMapper);
    }

    @Test
    void consume_PaymentCompleted_UpdatesOrderStatusToConfirmed() throws Exception {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(orderId, "user-1", new BigDecimal("100.00"), "MYR", OrderStatus.PENDING);
        PaymentCompletedEvent event = new PaymentCompletedEvent(
            UUID.randomUUID(),
            UUID.randomUUID(),
            orderId,
            "TXN-123",
            Instant.now()
        );
        String payload = objectMapper.writeValueAsString(event);

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        consumer.consume(payload);

        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
        verify(orderRepository).save(order);
    }

    @Test
    void consume_PaymentFailed_UpdatesOrderStatusToCancelled() throws Exception {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(orderId, "user-1", new BigDecimal("15000.00"), "MYR", OrderStatus.PENDING);
        PaymentFailedEvent event = new PaymentFailedEvent(
            UUID.randomUUID(),
            orderId,
            "CREDIT_LIMIT_EXCEEDED",
            Instant.now()
        );
        String payload = objectMapper.writeValueAsString(event);

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        consumer.consume(payload);

        assertEquals(OrderStatus.CANCELLED, order.getStatus());
        verify(orderRepository).save(order);
    }

    @Test
    void consume_AlreadyConfirmed_SkipsUpdate() throws Exception {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(orderId, "user-1", new BigDecimal("100.00"), "MYR", OrderStatus.CONFIRMED);
        PaymentCompletedEvent event = new PaymentCompletedEvent(
            UUID.randomUUID(),
            UUID.randomUUID(),
            orderId,
            "TXN-123",
            Instant.now()
        );
        String payload = objectMapper.writeValueAsString(event);

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        consumer.consume(payload);

        verify(orderRepository, never()).save(any());
    }
}
