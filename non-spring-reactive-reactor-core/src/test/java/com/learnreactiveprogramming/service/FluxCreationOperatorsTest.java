package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;

@DisplayName("FluxCreationOperators")
class FluxCreationOperatorsTest {

    private final FluxCreationOperators ops = new FluxCreationOperators();

    @Test
    @DisplayName("fluxCreate emits 5 events and completes")
    void fluxCreate_emitsFiveEvents() {
        StepVerifier.create(ops.fluxCreate())
                .expectNext("event-1", "event-2", "event-3", "event-4", "event-5")
                .verifyComplete();
    }

    @Test
    @DisplayName("fluxPush emits pushed items and completes")
    void fluxPush_emitsPushedItems() {
        StepVerifier.create(ops.fluxPush())
                .expectNext("pushed-1", "pushed-2")
                .verifyComplete();
    }

    @Test
    @DisplayName("fluxGenerate emits integers 0 through 9")
    void fluxGenerate_emitsTenIntegers() {
        StepVerifier.create(ops.fluxGenerate())
                .expectNext(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
                .verifyComplete();
    }

    @Test
    @DisplayName("fluxGenerateWithCleanup emits squares and completes")
    void fluxGenerateWithCleanup_emitsSquares() {
        StepVerifier.create(ops.fluxGenerateWithCleanup())
                .expectNext(0, 1, 4, 9, 16)
                .verifyComplete();
    }

    @Test
    @DisplayName("intervalExample emits 5 longs using virtual time")
    void intervalExample_emitsFiveLongs() {
        StepVerifier.withVirtualTime(ops::intervalExample)
                .expectSubscription()
                .thenAwait(Duration.ofMillis(600))
                .expectNext(0L, 1L, 2L, 3L, 4L)
                .verifyComplete();
    }
}
