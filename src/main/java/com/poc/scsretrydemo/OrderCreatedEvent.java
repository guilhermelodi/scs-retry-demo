package com.poc.scsretrydemo;

public record OrderCreatedEvent(
        Long id,
        Integer value,
        String status
) {
}
