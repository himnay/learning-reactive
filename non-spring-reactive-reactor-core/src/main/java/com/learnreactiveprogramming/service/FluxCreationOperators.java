package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demonstrates programmatic Flux creation operators useful when bridging
 * callback-based or pull-based APIs into the reactive world.
 */
public class FluxCreationOperators {

    /**
     * Flux.create bridges a multi-threaded callback API.
     * The FluxSink is thread-safe — multiple threads can call sink.next() concurrently.
     * Use when adapting listeners, event buses, or async callbacks.
     */
    public Flux<String> fluxCreate() {
        return Flux.create(sink -> {
            // Simulate a listener-based API emitting events
            for (int i = 1; i <= 5; i++) {
                sink.next("event-" + i);
            }
            sink.complete();
        });
    }

    /**
     * Flux.push is single-threaded: only one thread may call sink.next() at a time.
     * Use when the source is inherently single-threaded (e.g., a legacy callback that
     * fires on one dedicated thread).  Slightly less overhead than create().
     */
    public Flux<String> fluxPush() {
        return Flux.push(sink -> {
            sink.next("pushed-1");
            sink.next("pushed-2");
            sink.complete();
        });
    }

    /**
     * Flux.generate is a synchronous, pull-based generator.
     * The state (Integer) is passed between iterations — no shared mutable state needed.
     * The sink may only call next() ONCE per invocation of the generator (synchronous 1-by-1).
     */
    public Flux<Integer> fluxGenerate() {
        return Flux.generate(
                () -> 0,                             // initial state
                (state, sink) -> {
                    sink.next(state);                // emit current value
                    if (state == 9) sink.complete(); // stop after 10 items (0..9)
                    return state + 1;                // next state
                });
    }

    /**
     * Generator with conditional completion and a cleanup consumer.
     * The cleanup consumer runs regardless of whether the stream completed normally,
     * errored, or was cancelled — analogous to try-finally.
     */
    public Flux<Integer> fluxGenerateWithCleanup() {
        AtomicInteger externalResource = new AtomicInteger(0);
        return Flux.generate(
                () -> {
                    externalResource.set(100); // "open" resource
                    return 0;
                },
                (state, sink) -> {
                    sink.next(state * state);
                    if (state >= 4) sink.complete();
                    return state + 1;
                },
                state -> {
                    // Cleanup: called on complete, error, or cancel
                    externalResource.set(0);
                    System.out.println("Generator cleaned up at state=" + state);
                });
    }

    /**
     * Flux.interval emits Long values (0, 1, 2, …) at a fixed cadence.
     * take(5) limits to 5 items.  In tests, use StepVerifier.withVirtualTime
     * to avoid real sleeping.
     */
    public Flux<Long> intervalExample() {
        return Flux.interval(Duration.ofMillis(100)).take(5);
    }
}
