package com.poc.scsretrydemo;

import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderCreatedConsumer implements Consumer<Message<OrderCreatedEvent>> {

    private final OrderService orderService;
    private final OrderRetryPublisher orderRetryPublisher;

    public OrderCreatedConsumer(OrderService orderService, OrderRetryPublisher orderRetryPublisher) {
        this.orderService = orderService;
        this.orderRetryPublisher = orderRetryPublisher;
    }

    @Override
    public void accept(Message<OrderCreatedEvent> message) {
        OrderCreatedEvent event = message.getPayload();
        try {
            orderService.process(event);
        } catch (RuntimeException exception) {
            log.warn("Falha no consumo inicial da ordem {}. Encaminhando para retry.", event.id(), exception);
            orderRetryPublisher.sendToRetry(event, 1);
        }
    }
}
