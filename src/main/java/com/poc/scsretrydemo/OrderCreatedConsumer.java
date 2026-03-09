package com.poc.scsretrydemo;

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

    public OrderCreatedConsumer(OrderService orderService) {
        this.orderService = orderService;
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
            topics = "${spring.kafka.topic.order-created}"
    )
    public void onMessage(OrderCreatedEvent event) {
        log.info("Evento recebido para consumo: id={}, status={}, value={}",
                event.id(), event.status(), event.value());
        orderService.process(event);
    }

    @DltHandler
    public void onDlt(OrderCreatedEvent event) {
        log.error("Mensagem enviada para DLT: id={}, status={}, value={}",
                event.id(), event.status(), event.value());
    }
}
