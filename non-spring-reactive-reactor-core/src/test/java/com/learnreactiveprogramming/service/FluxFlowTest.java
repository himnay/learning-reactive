package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class FluxFlowTest {

    private FluxFlow fluxFlow = new FluxFlow();

    @Test
    @DisplayName("expectNext() using flux stream")
    void fluxStreamEventTest() {
        var stringFlux = fluxFlow.fluxPublisher();

        StepVerifier.create(stringFlux)
                .expectNext("******Himansu******", "******Sanjay******")
                .verifyComplete();
    }

    @Test
    @DisplayName("expectNextCount() using flux stream")
    void fluxStreamEventCountTest() {
        Flux<String> stringFlux = fluxFlow.fluxPublisher();

        StepVerifier.create(stringFlux)
                .expectNextCount(2);
    }

    @Test
    @DisplayName("expectNext() and expectNextCount() using flux stream")
    void fluxStreamEventEventAndCountTest() {
        var stringFlux = fluxFlow.fluxPublisher();

        StepVerifier.create(stringFlux)
                .expectNext("******Himansu******")
                .expectNextCount(1);
    }

    @Test
    @DisplayName("transform() using flux stream")
    void fluxStreamEventTransformTest() {
        var stringFlux = fluxFlow.fluxTransformPublisher();

        StepVerifier.create(stringFlux)
                .expectNext("HIMANSU")
                .verifyComplete();
    }

    @Test
    @DisplayName("filter() using flux stream")
    void fluxStreamEventFilterTest() {
        var stringFlux = fluxFlow.fluxFilterPublisher("himansu");

        StepVerifier.create(stringFlux)
                .expectNext("******Himansu******")
                .verifyComplete();
    }

    @Test
    @DisplayName("flatMap() using flux stream")
    void fluxStreamEventFlatMapTest() {
        var stringFlux = fluxFlow.fluxFlatMapPublisher();

        StepVerifier.create(stringFlux)
                .expectNext("H", "i", "m", "a", "n", "s", "u", "N", "a", "y", "a", "k")
                .verifyComplete();
    }

    @Test
    @DisplayName("flatMap() using flux stream")
    void fluxStreamEventFlatMapDelayTest() {
        var stringFlux = fluxFlow.fluxFlatMapPublisher();

        StepVerifier.create(stringFlux)
                .expectNext("H", "i", "m", "a", "n", "s", "u", "N", "a", "y", "a", "k")
                .expectError();

        StepVerifier.create(stringFlux)
                .expectNextCount(12)
                .verifyComplete();
    }

    @Test
    @DisplayName("transform() using flux stream")
    void monoEventAssertTest() {
        Flux<String> stringMono = fluxFlow.fluxTransformPublisher();

        StepVerifier.create(stringMono)
                .expectNext("HIMANSU")
                .verifyComplete();
    }

    @Test
    @DisplayName("defaultIfEmpty() using flux stream")
    void fluxDefaultIfEmptyTest() {
        Flux<String> stringMono = fluxFlow.fluxEmptyPublisher();

        StepVerifier.create(stringMono)
                .expectNext("default")
                .verifyComplete();
    }

    @Test
    @DisplayName("switchIfEmpty() using flux stream")
    void fluxSwitchIfEmptyTest() {
        Flux<String> stringMono = fluxFlow.fluxSwitchIfEmptyPublisher();

        StepVerifier.create(stringMono)
                .expectNext("default")
                .verifyComplete();
    }
}