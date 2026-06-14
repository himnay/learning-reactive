package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class MonoFlowTest {

    private MonoFlow monoFlow = new MonoFlow();

    @Test
    @DisplayName("expectNextCount() using mono stream")
    public void monoEventTest() {
        Mono<String> stringMono = monoFlow.monoPublisher();

        StepVerifier.create(stringMono)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    @DisplayName("expectNext() using mono stream")
    public void monoEventAssertTest() {
        Mono<String> stringMono = monoFlow.monoPublisher();

        StepVerifier.create(stringMono)
                .expectNext("******Alex******")
                .verifyComplete();
    }
}
