package com.poc.scsretrydemo;

import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderCreatedConsumer implements Consumer<OrderCreatedEvent> {

    @Override
    public void accept(OrderCreatedEvent event) {
        log.info("Mensagem consumida do tópico order-created: {}", event);
    }
}
