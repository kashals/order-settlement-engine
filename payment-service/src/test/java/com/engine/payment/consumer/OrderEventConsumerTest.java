package com.engine.payment.consumer;

import com.engine.common.OrderCreatedEvent;
import com.engine.payment.repository.ProcessedEventRepository;
import com.engine.payment.service.PaymentProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderEventConsumerTest {

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @Mock
    private PaymentProcessor paymentProcessor;

    private ObjectMapper objectMapper;
    private OrderEventConsumer consumer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        consumer = new OrderEventConsumer(processedEventRepository, paymentProcessor, objectMapper);
    }

    @Test
    void consume_NewEvent_ProcessesPayment() throws Exception {
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = new OrderCreatedEvent(
            UUID.randomUUID(),
            orderId,
            "user-1",
            new BigDecimal("150.00"),
            "MYR",
            Instant.now()
        );
        String payload = objectMapper.writeValueAsString(event);
        String expectedKey = orderId + ":ORDER_CREATED";

        when(processedEventRepository.insertIfNotExists(eq(expectedKey), eq("payment-service"))).thenReturn(1);

        consumer.consume(payload);

        verify(paymentProcessor).processPayment(any(OrderCreatedEvent.class));
    }

    @Test
    void consume_DuplicateEvent_SkipsExecution() throws Exception {
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = new OrderCreatedEvent(
            UUID.randomUUID(),
            orderId,
            "user-1",
            new BigDecimal("150.00"),
            "MYR",
            Instant.now()
        );
        String payload = objectMapper.writeValueAsString(event);
        String expectedKey = orderId + ":ORDER_CREATED";

        when(processedEventRepository.insertIfNotExists(eq(expectedKey), eq("payment-service"))).thenReturn(0);

        consumer.consume(payload);

        verify(paymentProcessor, never()).processPayment(any());
    }

    @Test
    void consume_MalformedPayload_ThrowsExceptionForDlqRouting() {
        String invalidPayload = "not-a-valid-json";

        assertThrows(IllegalArgumentException.class, () -> consumer.consume(invalidPayload));
        verify(paymentProcessor, never()).processPayment(any());
    }
}
