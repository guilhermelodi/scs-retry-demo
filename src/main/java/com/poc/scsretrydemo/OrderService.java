package com.poc.scsretrydemo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderService {

    public void process(OrderCreatedEvent event) {
        if ("ERROR".equalsIgnoreCase(event.status())) {
            throw new IllegalStateException("Falha simulada no processamento do pedido " + event.id());
        }

        log.info("Pedido processado com sucesso: id={}, status={}, value={}",
                event.id(), event.status(), event.value());
    }
}
