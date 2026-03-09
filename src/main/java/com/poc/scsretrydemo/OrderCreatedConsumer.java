package com.poc.scsretrydemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderCreatedConsumer {

    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    public OrderCreatedConsumer(OrderService orderService, ObjectMapper objectMapper) {
        this.orderService = orderService;
        this.objectMapper = objectMapper;
    }

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 30_000),
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            autoCreateTopics = "false"
    )
    @KafkaListener(
            topics = "${spring.kafka.topic.order-created}",
            groupId = "${spring.application.name}"
    )
    public void onMessage(String payload) {
        orderService.process(readEvent(payload));
    }

    @DltHandler
    public void onDlt(String payload) {
        OrderCreatedEvent event = readEvent(payload);
        log.error("Mensagem enviada para DLT: id={}, status={}, value={}",
                event.id(), event.status(), event.value());
    }

    private OrderCreatedEvent readEvent(String payload) {
        try {
            return objectMapper.readValue(payload, OrderCreatedEvent.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Payload inválido para OrderCreatedEvent", exception);
        }
    }
}
