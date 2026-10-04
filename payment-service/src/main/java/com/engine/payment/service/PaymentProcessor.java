package com.engine.payment.service;

import com.engine.common.OrderCreatedEvent;
import com.engine.common.PaymentCompletedEvent;
import com.engine.common.PaymentFailedEvent;
import com.engine.payment.domain.Payment;
import com.engine.payment.domain.PaymentStatus;
import com.engine.payment.repository.PaymentRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Service
public class PaymentProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessor.class);
    private static final String PAYMENT_TOPIC = "payment.events";
    private static final BigDecimal MAX_ALLOWED_AMOUNT = new BigDecimal("10000.00");

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public PaymentProcessor(
        PaymentRepository paymentRepository,
        KafkaTemplate<String, String> kafkaTemplate,
        ObjectMapper objectMapper
    ) {
        this.paymentRepository = paymentRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void processPayment(OrderCreatedEvent event) {
        log.info("Processing payment for orderId={}", event.orderId());

        if (event.totalAmount().compareTo(MAX_ALLOWED_AMOUNT) > 0) {
            Payment payment = new Payment(
                UUID.randomUUID(),
                event.orderId(),
                event.totalAmount(),
                PaymentStatus.FAILED,
                null
            );
            paymentRepository.save(payment);

            PaymentFailedEvent failedEvent = new PaymentFailedEvent(
                UUID.randomUUID(),
                event.orderId(),
                "CREDIT_LIMIT_EXCEEDED",
                Instant.now()
            );

            try {
                String payload = objectMapper.writeValueAsString(failedEvent);
                kafkaTemplate.send(PAYMENT_TOPIC, event.orderId().toString(), payload);
                log.info("Published PaymentFailedEvent for orderId={}", event.orderId());
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Failed to serialize PaymentFailedEvent", e);
            }
            return;
        }

        UUID paymentId = UUID.randomUUID();
        String txnRef = "TXN-" + UUID.randomUUID();

        Payment payment = new Payment(
            paymentId,
            event.orderId(),
            event.totalAmount(),
            PaymentStatus.SUCCESS,
            txnRef
        );
        paymentRepository.save(payment);

        PaymentCompletedEvent completedEvent = new PaymentCompletedEvent(
            UUID.randomUUID(),
            paymentId,
            event.orderId(),
            txnRef,
            Instant.now()
        );

        try {
            String payload = objectMapper.writeValueAsString(completedEvent);
            kafkaTemplate.send(PAYMENT_TOPIC, event.orderId().toString(), payload);
            log.info("Published PaymentCompletedEvent for orderId={}", event.orderId());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize PaymentCompletedEvent", e);
        }
    }
}
