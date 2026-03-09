package com.poc.scsretrydemo;

import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class OrderCreatedConsumer {

    @Bean
    public Consumer<OrderCreatedEvent> orderCreatedConsumer() {
        return event -> log.info("Mensagem consumida do tópico order-created: {}", event);
    }
}
