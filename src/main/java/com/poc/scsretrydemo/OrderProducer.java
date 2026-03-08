package com.poc.scsretrydemo;

import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Component;

@Component
public class OrderProducer {

    private static final String OUTPUT_BINDING = "order-created-out-0";

    private final StreamBridge streamBridge;

    public OrderProducer(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    public boolean publish(OrderCreatedEvent event) {
        return streamBridge.send(OUTPUT_BINDING, event);
    }
}
