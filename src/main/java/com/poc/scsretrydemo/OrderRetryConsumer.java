package com.poc.scsretrydemo;

import java.time.Instant;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

@Component
public class OrderRetryConsumer implements Consumer<Message<OrderCreatedEvent>> {

    private static final Logger log = LoggerFactory.getLogger(OrderRetryConsumer.class);

    static final String RETRY_ATTEMPT_HEADER = "x-retry-attempt";
    static final String ERROR_MESSAGE_HEADER = "x-error-message";

    static final String RETRY_OUTPUT_BINDING = "order-created-retry-out-0";
    static final String DLT_OUTPUT_BINDING = "order-created-dlt-out-0";

    static final int MAX_RETRIES = 3;
    static final int RETRY_INTERVAL_SECONDS = 30;

    private final StreamBridge streamBridge;
    private final TaskScheduler taskScheduler;

    public OrderRetryConsumer(StreamBridge streamBridge, TaskScheduler taskScheduler) {
        this.streamBridge = streamBridge;
        this.taskScheduler = taskScheduler;
    }

    @Override
    public void accept(Message<OrderCreatedEvent> message) {
        OrderCreatedEvent event = message.getPayload();
        int attempt = message.getHeaders().get(RETRY_ATTEMPT_HEADER, Integer.class) == null
                ? 1
                : message.getHeaders().get(RETRY_ATTEMPT_HEADER, Integer.class);

        try {
            log.info("Consuming retry event. orderId={}, attempt={}", event.id(), attempt);
            processEvent(event);
            log.info("Retry event processed with success. orderId={}, attempt={}", event.id(), attempt);
        } catch (Exception ex) {
            log.error("Error processing retry event. orderId={}, attempt={}", event.id(), attempt, ex);
            handleFailedOrder(event, attempt, ex.getMessage());
        }
    }

    public void handleFailedOrder(OrderCreatedEvent event, int currentAttempt, String errorMessage) {
        int nextAttempt = currentAttempt + 1;

        if (nextAttempt <= MAX_RETRIES) {
            Instant retryAt = Instant.now().plusSeconds(RETRY_INTERVAL_SECONDS);
            log.warn("Scheduling order event retry. orderId={}, nextAttempt={}, retryAt={}, error={}",
                    event.id(), nextAttempt, retryAt, errorMessage);

            taskScheduler.schedule(() -> {
                boolean sent = streamBridge.send(RETRY_OUTPUT_BINDING,
                        MessageBuilder.withPayload(event)
                                .setHeader(RETRY_ATTEMPT_HEADER, nextAttempt)
                                .setHeader(ERROR_MESSAGE_HEADER, errorMessage)
                                .build());

                if (sent) {
                    log.info("Order event sent to retry topic. orderId={}, attempt={}", event.id(), nextAttempt);
                    return;
                }

                log.error("Failed to send order event to retry topic. orderId={}, attempt={}", event.id(), nextAttempt);
            }, retryAt);
            return;
        }

        boolean dltSent = streamBridge.send(DLT_OUTPUT_BINDING,
                MessageBuilder.withPayload(event)
                        .setHeader(RETRY_ATTEMPT_HEADER, currentAttempt)
                        .setHeader(ERROR_MESSAGE_HEADER, errorMessage)
                        .build());

        if (dltSent) {
            log.error("Max retries exceeded. Sending order event to DLT. orderId={}, attempts={}", event.id(), currentAttempt);
            return;
        }

        log.error("Max retries exceeded and DLT publish failed. orderId={}, attempts={}", event.id(), currentAttempt);
    }

    private void processEvent(OrderCreatedEvent event) {
        if ("ERROR".equalsIgnoreCase(event.status())) {
            throw new IllegalStateException("Failed processing order with status ERROR");
        }
    }
}
