package com.poc.scsretrydemo;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

class OrderCreatedConsumerTest {

    @Test
    void shouldConfigureRetryTopicAndDlt() throws NoSuchMethodException {
        Method consumeMethod = OrderCreatedConsumer.class.getMethod("consume", OrderCreatedEvent.class);

        RetryableTopic retryableTopic = consumeMethod.getAnnotation(RetryableTopic.class);
        KafkaListener kafkaListener = consumeMethod.getAnnotation(KafkaListener.class);

        assertThat(retryableTopic).isNotNull();
        assertThat(retryableTopic.attempts()).isEqualTo("4");
        assertThat(retryableTopic.backoff().delay()).isEqualTo(30_000L);
        assertThat(retryableTopic.retryTopicSuffix()).isEqualTo("-retry");
        assertThat(retryableTopic.dltTopicSuffix()).isEqualTo("-dlt");

        assertThat(kafkaListener).isNotNull();
        assertThat(kafkaListener.topics()).containsExactly("${app.kafka.consumer.order-created.topic}");
        assertThat(kafkaListener.groupId()).isEqualTo("${app.kafka.consumer.order-created.group-id}");
    }
}
