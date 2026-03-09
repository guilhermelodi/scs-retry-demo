package com.poc.scsretrydemo;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.scheduling.TaskScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderRetryConsumerTest {

    @Test
    void shouldScheduleRetryWhenAttemptsAreBelowLimit() {
        StreamBridge streamBridge = mock(StreamBridge.class);
        TaskScheduler taskScheduler = mock(TaskScheduler.class);
        OrderRetryConsumer orderRetryConsumer = new OrderRetryConsumer(streamBridge, taskScheduler);

        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(taskScheduler.schedule(runnableCaptor.capture(), any(Instant.class))).thenReturn(null);
        when(streamBridge.send(eq(OrderRetryConsumer.RETRY_OUTPUT_BINDING), any(Message.class))).thenReturn(true);

        orderRetryConsumer.handleFailedOrder(new OrderCreatedEvent(1L, 100, "ERROR"), 0, "boom");

        verify(taskScheduler).schedule(any(Runnable.class), any(Instant.class));

        Runnable runnable = runnableCaptor.getValue();
        runnable.run();

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(streamBridge).send(eq(OrderRetryConsumer.RETRY_OUTPUT_BINDING), messageCaptor.capture());
        assertThat(messageCaptor.getValue().getHeaders().get(OrderRetryConsumer.RETRY_ATTEMPT_HEADER)).isEqualTo(1);
    }

    @Test
    void shouldSendToDltWhenRetriesAreExhausted() {
        StreamBridge streamBridge = mock(StreamBridge.class);
        TaskScheduler taskScheduler = mock(TaskScheduler.class);
        OrderRetryConsumer orderRetryConsumer = new OrderRetryConsumer(streamBridge, taskScheduler);

        when(streamBridge.send(eq(OrderRetryConsumer.DLT_OUTPUT_BINDING), any(Message.class))).thenReturn(true);

        orderRetryConsumer.handleFailedOrder(new OrderCreatedEvent(2L, 100, "ERROR"), 3, "boom");

        verify(streamBridge).send(eq(OrderRetryConsumer.DLT_OUTPUT_BINDING), any(Message.class));
    }
}
