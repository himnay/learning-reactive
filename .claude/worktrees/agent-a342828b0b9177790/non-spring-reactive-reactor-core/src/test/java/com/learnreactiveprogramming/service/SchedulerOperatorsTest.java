package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("SchedulerOperators")
class SchedulerOperatorsTest {

    private final SchedulerOperators ops = new SchedulerOperators();

    @Test
    @DisplayName("subscribeOnBoundedElastic runs the source on boundedElastic thread")
    void subscribeOnBoundedElastic_runsOnCorrectThread() {
        var result = new String[1];

        StepVerifier.create(
                ops.subscribeOnBoundedElastic()
                   .doOnNext(v -> result[0] = v))
                .expectNextMatches(v -> v.equals("result"))
                .verifyComplete();

        assertThat(result[0]).isEqualTo("result");
    }

    @Test
    @DisplayName("publishOnParallel maps items and completes")
    void publishOnParallel_mapsItems() {
        StepVerifier.create(ops.publishOnParallel())
                .expectNextSequence(java.util.List.of(2, 4, 6, 8, 10))
                .verifyComplete();
    }

    @Test
    @DisplayName("subscribeOnVsPublishOn produces uppercase items")
    void subscribeOnVsPublishOn_producesUppercase() {
        StepVerifier.create(ops.subscribeOnVsPublishOn())
                .expectNextMatches(s -> s.equals(s.toUpperCase()))
                .expectNextMatches(s -> s.equals(s.toUpperCase()))
                .expectNextMatches(s -> s.equals(s.toUpperCase()))
                .verifyComplete();
    }

    @Test
    @DisplayName("wrapBlockingCall returns result from blocking supplier")
    void wrapBlockingCall_returnsResult() {
        StepVerifier.create(ops.wrapBlockingCall())
                .expectNextMatches(v -> v.startsWith("legacy-result-"))
                .verifyComplete();
    }
}
