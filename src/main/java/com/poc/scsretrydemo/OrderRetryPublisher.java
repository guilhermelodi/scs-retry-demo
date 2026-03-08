package com.poc.scsretrydemo;

import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.cloud.stream.function.StreamBridge;

@Component
public class OrderRetryPublisher {

    static final String RETRY_OUTPUT_BINDING = "orderCreatedRetryProducer-out-0";
    static final String DLT_OUTPUT_BINDING = "orderCreatedDltProducer-out-0";
    static final String ATTEMPT_HEADER = "x-retry-attempt";
    static final String ERROR_HEADER = "x-error-message";

    private final StreamBridge streamBridge;

    public OrderRetryPublisher(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    public boolean sendToRetry(OrderCreatedEvent event, int attempt) {
        Message<OrderCreatedEvent> message = MessageBuilder.withPayload(event)
                .setHeader(ATTEMPT_HEADER, attempt)
                .build();

        return streamBridge.send(RETRY_OUTPUT_BINDING, message);
    }

    public boolean sendToDlt(OrderCreatedEvent event, int attempt, String errorMessage) {
        Message<OrderCreatedEvent> message = MessageBuilder.withPayload(event)
                .setHeader(ATTEMPT_HEADER, attempt)
                .setHeader(ERROR_HEADER, errorMessage)
                .build();

        return streamBridge.send(DLT_OUTPUT_BINDING, message);
    }
}
