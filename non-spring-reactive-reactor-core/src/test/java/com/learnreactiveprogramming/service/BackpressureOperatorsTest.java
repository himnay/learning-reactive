package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

@DisplayName("BackpressureOperators")
class BackpressureOperatorsTest {

    private final BackpressureOperators ops = new BackpressureOperators();

    @Test
    @DisplayName("onBackpressureDrop does not throw and eventually completes")
    void onBackpressureDrop_completes() {
        StepVerifier.create(ops.onBackpressureDrop(), 10)
                .expectNextCount(10)
                .thenRequest(Long.MAX_VALUE)
                .thenConsumeWhile(i -> true)  // drain any extra items buffered before drop kicks in
                .verifyComplete();
    }

    @Test
    @DisplayName("limitRate emits all 100 items via throttled prefetch")
    void limitRate_emitsAllItems() {
        StepVerifier.create(ops.limitRate())
                .expectNextCount(100)
                .verifyComplete();
    }

    @Test
    @DisplayName("limitRequest emits exactly 5 items and completes")
    void limitRequest_emitsFiveItems() {
        StepVerifier.create(ops.limitRequest())
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    @DisplayName("onBackpressureLatest emits some items and completes")
    void onBackpressureLatest_emitsAndCompletes() {
        StepVerifier.create(ops.onBackpressureLatest(), 5)
                .expectNextCount(5)
                .thenRequest(Long.MAX_VALUE)
                .thenConsumeWhile(i -> true)  // drain the "latest" buffered item (the last unseen value)
                .verifyComplete();
    }
}
