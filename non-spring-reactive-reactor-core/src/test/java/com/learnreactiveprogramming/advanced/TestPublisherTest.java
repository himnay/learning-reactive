package com.learnreactiveprogramming.advanced;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("TestPublisher for controlled publisher scenarios")
class TestPublisherTest {

    @Test
    @DisplayName("TestPublisher emits items programmatically to a downstream verifier")
    void testPublisherControl_emitsItems() {
        TestPublisher<String> publisher = TestPublisher.create();

        StepVerifier.create(publisher.flux())
                .then(() -> {
                    publisher.next("hello");
                    publisher.next("world");
                    publisher.complete();
                })
                .expectNext("hello", "world")
                .verifyComplete();
    }

    @Test
    @DisplayName("TestPublisher can emit an error to test error handling")
    void testPublisher_emitsError() {
        TestPublisher<String> publisher = TestPublisher.create();
        RuntimeException error = new RuntimeException("test error");

        StepVerifier.create(publisher.flux().onErrorReturn("fallback"))
                .then(() -> publisher.error(error))
                .expectNext("fallback")
                .verifyComplete();
    }

    @Test
    @DisplayName("TestPublisher tracks subscription assertions")
    void testPublisher_subscriptionAssertions() {
        TestPublisher<Integer> publisher = TestPublisher.create();

        Flux<Integer> mapped = publisher.flux().map(n -> n * 2);

        StepVerifier.create(mapped)
                .then(() -> {
                    publisher.assertMinRequested(1);  // subscriber has already issued demand
                    publisher.next(1, 2, 3).complete();
                })
                .expectNext(2, 4, 6)
                .verifyComplete();
    }

    @Test
    @DisplayName("TestPublisher cancellation propagates to publisher")
    void testPublisher_cancellation() {
        TestPublisher<String> publisher = TestPublisher.create();

        StepVerifier.create(publisher.flux().take(1))
                .then(() -> publisher.next("only-one"))
                .expectNext("only-one")
                .verifyComplete();

        // After take(1) completes, the subscription is cancelled — no active subscribers remain
        publisher.assertNoSubscribers();
    }
}
