package com.poc.scsretrydemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrderCreatedConsumerTest {

    private final OrderService orderService = mock(OrderService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OrderCreatedConsumer consumer = new OrderCreatedConsumer(orderService, objectMapper);

    @Test
    void shouldProcessOrderWhenPayloadIsValid() {
        consumer.onMessage("""
                {
                  "id": 1,
                  "value": 250,
                  "status": "CREATED"
                }
                """);

        verify(orderService).process(new OrderCreatedEvent(1L, 250, "CREATED"));
    }

    @Test
    void shouldThrowWhenPayloadIsInvalid() {
        assertThrows(IllegalArgumentException.class, () -> consumer.onMessage("invalid-json"));
    }
}
