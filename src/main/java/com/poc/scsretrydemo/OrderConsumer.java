package com.poc.scsretrydemo;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer implements Consumer<Message<OrderCreatedEvent>> {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final OrderRetryConsumer orderRetryConsumer;

    public OrderConsumer(OrderRetryConsumer orderRetryConsumer) {
        this.orderRetryConsumer = orderRetryConsumer;
    }

    @Override
    public void accept(Message<OrderCreatedEvent> message) {
        OrderCreatedEvent event = message.getPayload();

        try {
            log.info("Consuming order-created event. orderId={}", event.id());
            processEvent(event);
            log.info("Order-created event processed with success. orderId={}", event.id());
        } catch (Exception ex) {
            log.error("Error processing order-created event. orderId={}", event.id(), ex);
            orderRetryConsumer.handleFailedOrder(event, 0, ex.getMessage());
        }
    }

    private void processEvent(OrderCreatedEvent event) {
        if ("ERROR".equalsIgnoreCase(event.status())) {
            throw new IllegalStateException("Failed processing order with status ERROR");
        }
    }
}
