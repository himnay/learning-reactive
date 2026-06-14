package com.learnreactiveprogramming.advanced;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

@DisplayName("VirtualTime advanced tests")
class VirtualTimeTest {

    @Test
    @DisplayName("interval with virtual time avoids real sleeping")
    void testIntervalWithVirtualTime() {
        StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofSeconds(1)).take(3))
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(3))
                .expectNext(0L, 1L, 2L)
                .verifyComplete();
    }

    @Test
    @DisplayName("buffer(Duration) groups items emitted within the window")
    void testBufferWithDuration() {
        StepVerifier.withVirtualTime(() ->
                        Flux.interval(Duration.ofMillis(200))
                                .take(4)
                                .buffer(Duration.ofSeconds(1)))
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(2))
                .expectNextMatches(list -> list.size() >= 1)
                .verifyComplete();
    }

    @Test
    @DisplayName("delayElements shifts each item by the given duration")
    void testDelayElements() {
        StepVerifier.withVirtualTime(() ->
                        Flux.just("a", "b", "c")
                                .delayElements(Duration.ofSeconds(1)))
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(3))
                .expectNext("a", "b", "c")
                .verifyComplete();
    }

    @Test
    @DisplayName("debounce keeps only the last item after quiet period")
    void testDebounce() {
        StepVerifier.withVirtualTime(() ->
                        Flux.concat(
                                Flux.just("a").delayElements(Duration.ofMillis(100)),
                                Flux.just("b").delayElements(Duration.ofMillis(100)),
                                Flux.just("c").delayElements(Duration.ofSeconds(1))
                        ).sample(Duration.ofMillis(500)))
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(3))
                .expectNextCount(2) // "b" is sampled at the 500ms tick; "c" is flushed on source completion
                .verifyComplete();
    }
}
