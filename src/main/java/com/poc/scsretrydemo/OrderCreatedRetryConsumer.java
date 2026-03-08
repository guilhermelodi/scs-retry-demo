package com.poc.scsretrydemo;

import java.time.Instant;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderCreatedRetryConsumer implements Consumer<Message<OrderCreatedEvent>> {

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final int RETRY_DELAY_SECONDS = 30;

    private final OrderService orderService;
    private final OrderRetryPublisher orderRetryPublisher;
    private final TaskScheduler taskScheduler;

    public OrderCreatedRetryConsumer(
            OrderService orderService,
            OrderRetryPublisher orderRetryPublisher,
            TaskScheduler taskScheduler
    ) {
        this.orderService = orderService;
        this.orderRetryPublisher = orderRetryPublisher;
        this.taskScheduler = taskScheduler;
    }

    @Override
    public void accept(Message<OrderCreatedEvent> message) {
        OrderCreatedEvent event = message.getPayload();
        int currentAttempt = resolveAttempt(message);

        log.info("Mensagem recebida no tópico de retry para ordem {}. Tentativa {} de {}.",
                event.id(), currentAttempt, MAX_RETRY_ATTEMPTS);

        taskScheduler.schedule(
                () -> processRetryAttempt(event, currentAttempt),
                Instant.now().plusSeconds(RETRY_DELAY_SECONDS)
        );
    }

    private void processRetryAttempt(OrderCreatedEvent event, int currentAttempt) {
        try {
            orderService.process(event);
        } catch (RuntimeException exception) {
            if (currentAttempt < MAX_RETRY_ATTEMPTS) {
                int nextAttempt = currentAttempt + 1;
                log.warn("Falha na tentativa {} para ordem {}. Reenviando para retry.",
                        currentAttempt, event.id(), exception);
                orderRetryPublisher.sendToRetry(event, nextAttempt);
                return;
            }

            log.error("Tentativas esgotadas para ordem {}. Enviando para DLT.", event.id(), exception);
            orderRetryPublisher.sendToDlt(event, currentAttempt, exception.getMessage());
        }
    }

    private int resolveAttempt(Message<OrderCreatedEvent> message) {
        Object attemptHeader = message.getHeaders().get(OrderRetryPublisher.ATTEMPT_HEADER);
        if (attemptHeader instanceof Number attempt) {
            return attempt.intValue();
        }

        return 1;
    }
}
