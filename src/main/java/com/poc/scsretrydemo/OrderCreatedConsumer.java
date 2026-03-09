package com.poc.scsretrydemo;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OrderCreatedConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderCreatedConsumer.class);

    @Bean
    public Consumer<OrderCreatedEvent> orderCreatedConsumer() {
        return event -> LOGGER.info("Mensagem consumida do tópico order-created: {}", event);
    }
}
