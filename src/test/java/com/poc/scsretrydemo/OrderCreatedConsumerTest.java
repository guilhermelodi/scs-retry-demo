package com.poc.scsretrydemo;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrderCreatedConsumerTest {

    private final OrderService orderService = mock(OrderService.class);
    private final OrderCreatedConsumer consumer = new OrderCreatedConsumer(orderService);

    @Test
    void shouldProcessOrderWhenEventIsReceived() {
        OrderCreatedEvent event = new OrderCreatedEvent(1L, 250, "CREATED");

        consumer.onMessage(event);

        verify(orderService).process(event);
    }
}
