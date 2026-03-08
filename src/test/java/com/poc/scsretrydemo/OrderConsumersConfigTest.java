package com.poc.scsretrydemo;

import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ScheduledFuture;

import org.junit.jupiter.api.Test;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrderConsumersConfigTest {

    private final OrderService orderService = mock(OrderService.class);
    private final OrderRetryPublisher orderRetryPublisher = mock(OrderRetryPublisher.class);
    private final TaskScheduler taskScheduler = new ImmediateTaskScheduler();

    private final OrderCreatedConsumer orderCreatedConsumer = new OrderCreatedConsumer(orderService, orderRetryPublisher);
    private final OrderCreatedRetryConsumer orderCreatedRetryConsumer = new OrderCreatedRetryConsumer(
            orderService,
            orderRetryPublisher,
            taskScheduler
    );

    @Test
    void shouldSendToRetryWhenInitialConsumerFails() {
        OrderCreatedEvent event = new OrderCreatedEvent(1L, 250, "ERROR");
        doThrow(new IllegalStateException("falha")).when(orderService).process(event);

        orderCreatedConsumer.accept(MessageBuilder.withPayload(event).build());

        verify(orderRetryPublisher).sendToRetry(event, 1);
    }

    @Test
    void shouldSendToNextRetryAttemptWhenRetryFailsAndAttemptsRemain() {
        OrderCreatedEvent event = new OrderCreatedEvent(1L, 250, "ERROR");
        doThrow(new IllegalStateException("falha")).when(orderService).process(event);

        orderCreatedRetryConsumer.accept(MessageBuilder.withPayload(event)
                .setHeader(OrderRetryPublisher.ATTEMPT_HEADER, 1)
                .build());

        verify(orderRetryPublisher).sendToRetry(event, 2);
    }

    @Test
    void shouldSendToDltWhenRetryLimitIsReached() {
        OrderCreatedEvent event = new OrderCreatedEvent(1L, 250, "ERROR");
        doThrow(new IllegalStateException("falha")).when(orderService).process(event);

        orderCreatedRetryConsumer.accept(MessageBuilder.withPayload(event)
                .setHeader(OrderRetryPublisher.ATTEMPT_HEADER, 3)
                .build());

        verify(orderRetryPublisher).sendToDlt(event, 3, "falha");
    }

    private static class ImmediateTaskScheduler implements TaskScheduler {

        @Override
        public ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
            task.run();
            return null;
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable task, Instant startTime) {
            task.run();
            return null;
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable task, Date startTime) {
            task.run();
            return null;
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Instant startTime, java.time.Duration period) {
            task.run();
            return null;
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Date startTime, long period) {
            task.run();
            return null;
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long period) {
            task.run();
            return null;
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Instant startTime, java.time.Duration delay) {
            task.run();
            return null;
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Date startTime, long delay) {
            task.run();
            return null;
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long delay) {
            task.run();
            return null;
        }
    }
}
