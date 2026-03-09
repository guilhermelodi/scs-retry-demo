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

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 30_000),
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC
    )
    @KafkaListener(
            topics = "${spring.kafka.consumer.topic.order-created}",
            groupId = "${spring.application.name}"
    )
    public void consume(OrderCreatedEvent event) {
        log.info("Received order-created event: {}", event);

        if ("ERROR".equalsIgnoreCase(event.status())) {
            throw new IllegalStateException("Failed to process order " + event.id());
        }
    }

    @DltHandler
    public void handleDlt(OrderCreatedEvent event) {
        log.error("Order event sent to DLT after retries: {}", event);
    }
}
