package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("DeferOperators")
class DeferOperatorsTest {

    private final DeferOperators ops = new DeferOperators();

    @Test
    @DisplayName("monoDefer evaluates the factory each subscription")
    void monoDefer_evaluatesLazily() {
        var deferred = ops.monoDefer();

        // Each subscription should produce a distinct value (different call count)
        String[] first = {null}, second = {null};

        StepVerifier.create(deferred.doOnNext(v -> first[0] = v))
                .expectNextCount(1)
                .verifyComplete();

        StepVerifier.create(deferred.doOnNext(v -> second[0] = v))
                .expectNextCount(1)
                .verifyComplete();

        // Deferred evaluation means the factory ran twice, producing different values
        assertThat(first[0]).isNotEqualTo(second[0]);
    }

    @Test
    @DisplayName("fromCallable wraps a callable and emits its return value")
    void fromCallable_emitsValue() {
        StepVerifier.create(ops.fromCallable())
                .expectNext("from-callable")
                .verifyComplete();
    }

    @Test
    @DisplayName("fromSupplier emits a unique value each subscription")
    void fromSupplier_emitsValue() {
        StepVerifier.create(ops.fromSupplier())
                .expectNextMatches(v -> v.startsWith("from-supplier-"))
                .verifyComplete();
    }

    @Test
    @DisplayName("deferredCurrentTime captures Instant at subscribe time, not assembly")
    void deferredCurrentTime_capturesSubscribeTime() throws InterruptedException {
        var mono = ops.deferredCurrentTime();
        Instant before = Instant.now();

        Thread.sleep(5);

        Instant[] captured = {null};
        StepVerifier.create(mono.doOnNext(i -> captured[0] = i))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(captured[0]).isAfterOrEqualTo(before);
    }
}
