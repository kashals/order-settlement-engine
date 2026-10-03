package com.engine.payment.consumer;

import com.engine.common.OrderCreatedEvent;
import com.engine.payment.domain.ProcessedEvent;
import com.engine.payment.repository.ProcessedEventRepository;
import com.engine.payment.service.PaymentProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
    private static final String CONSUMER_NAME = "payment-service";

    private final ProcessedEventRepository processedEventRepository;
    private final PaymentProcessor paymentProcessor;
    private final ObjectMapper objectMapper;

    public OrderEventConsumer(
        ProcessedEventRepository processedEventRepository,
        PaymentProcessor paymentProcessor,
        ObjectMapper objectMapper
    ) {
        this.processedEventRepository = processedEventRepository;
        this.paymentProcessor = paymentProcessor;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order.events", groupId = "payment-service-group")
    @Transactional
    public void consume(String message) {
        log.info("Received message from order.events: {}", message);

        OrderCreatedEvent event;
        try {
            event = objectMapper.readValue(message, OrderCreatedEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Malformed JSON payload, routing to DLQ", e);
        }

        // deduplication check
        String idempotencyKey = event.orderId().toString() + ":ORDER_CREATED";
        if (processedEventRepository.existsById(idempotencyKey)) {
            log.warn("Duplicate event skipped for idempotencyKey={}", idempotencyKey);
            return;
        }

        try {
            processedEventRepository.saveAndFlush(new ProcessedEvent(idempotencyKey, CONSUMER_NAME));
        } catch (DataIntegrityViolationException e) {
            log.warn("Duplicate event detected on concurrent insert for idempotencyKey={}", idempotencyKey);
            return;
        }

        paymentProcessor.processPayment(event);
    }
}
