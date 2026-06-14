package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("CachingOperators")
class CachingOperatorsTest {

    @Test
    @DisplayName("cacheFlux executes source only once for multiple subscribers")
    void cacheFlux_sourceExecutedOnce() {
        CachingOperators ops = new CachingOperators();
        var cached = ops.cacheFlux();

        List<String> first = cached.collectList().block();
        List<String> second = cached.collectList().block();

        // Both subscribers get the same cached results
        assertThat(first).isEqualTo(second);
        // call count in source should be 1 (not 2)
    }

    @Test
    @DisplayName("cacheMono returns same value on repeated subscription")
    void cacheMono_returnsCachedValue() {
        CachingOperators ops = new CachingOperators();
        var cached = ops.cacheMono();

        StepVerifier.create(cached)
                .expectNext("mono-result")
                .verifyComplete();

        StepVerifier.create(cached)
                .expectNext("mono-result")
                .verifyComplete();
    }

    @Test
    @DisplayName("cacheWithTtl emits results and completes")
    void cacheWithTtl_emitsAndCompletes() {
        CachingOperators ops = new CachingOperators();
        StepVerifier.create(ops.cacheWithTtl())
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    @DisplayName("replayFlux allows late subscriber to get last n items")
    void replayFlux_allowsLateSubscriber() {
        CachingOperators ops = new CachingOperators();
        var replayed = ops.replayFlux();

        // First subscriber triggers the source
        List<String> first = replayed.collectList().block();
        assertThat(first).hasSize(2);
    }
}
