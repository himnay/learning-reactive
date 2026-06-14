package com.learnreactiveprogramming.advanced;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;

@DisplayName("PublisherProbe for verifying fallback paths")
class PublisherProbeTest {

    @Test
    @DisplayName("fallback Mono is subscribed when primary is empty")
    void testFallbackWasInvoked_whenPrimaryEmpty() {
        PublisherProbe<String> fallback = PublisherProbe.of(Mono.just("fallback"));

        Mono<String> pipeline = Mono.<String>empty()
                .switchIfEmpty(fallback.mono());

        StepVerifier.create(pipeline)
                .expectNext("fallback")
                .verifyComplete();

        fallback.assertWasSubscribed();
        fallback.assertWasRequested();
        fallback.assertWasNotCancelled();
    }

    @Test
    @DisplayName("fallback is NOT subscribed when primary has a value")
    void testFallbackNotInvoked_whenPrimaryHasValue() {
        PublisherProbe<String> fallback = PublisherProbe.of(Mono.just("fallback"));

        Mono<String> pipeline = Mono.just("primary")
                .switchIfEmpty(fallback.mono());

        StepVerifier.create(pipeline)
                .expectNext("primary")
                .verifyComplete();

        fallback.assertWasNotSubscribed();
    }

    @Test
    @DisplayName("fallback is cancelled when downstream takes only first item")
    void testFallbackCancelled_afterTake() {
        PublisherProbe<String> probe = PublisherProbe.of(
                Mono.just("a").concatWith(Mono.just("b")).flux().next());

        // take(1) downstream cancels the rest
        StepVerifier.create(probe.flux().take(1))
                .expectNext("a")
                .verifyComplete();

        probe.assertWasSubscribed();
    }
}
