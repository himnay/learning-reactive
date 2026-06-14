package com.learnreactiveprogramming.service;

import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Caching and multicasting operators control when the upstream source is subscribed
 * and how many times the "expensive" operation is actually executed.
 *
 * cache(): replays to new subscribers; subscription happens once, results replayed.
 * share(): multicasts to current subscribers; new subscribers after completion get nothing.
 * replay(n): keeps n items in history for late subscribers.
 */
public class CachingOperators {

    private final AtomicInteger dbCallCount = new AtomicInteger(0);

    private Flux<String> expensiveDbQuery() {
        return Flux.defer(() -> {
            int callNo = dbCallCount.incrementAndGet();
            System.out.println("DB query executed, call #" + callNo);
            return Flux.just("row1-" + callNo, "row2-" + callNo);
        });
    }

    /**
     * cache(): the first subscriber triggers the upstream; subsequent subscribers
     * receive the cached (replayed) items without re-triggering the source.
     * Ideal for read-through caching where the first call populates the cache.
     */
    public Flux<String> cacheFlux() {
        return expensiveDbQuery().cache();
    }

    /**
     * cache() on Mono works identically: executes once, replays to all subscribers.
     */
    public Mono<String> cacheMono() {
        return Mono.defer(() -> {
            System.out.println("Mono DB query executed");
            return Mono.just("mono-result");
        }).cache();
    }

    /**
     * cache(Duration): cached items expire after the TTL.
     * After expiry, the next subscriber re-triggers the upstream source.
     */
    public Flux<String> cacheWithTtl() {
        return expensiveDbQuery().cache(Duration.ofSeconds(5));
    }

    /**
     * share(): creates a hot publisher that multicasts to ALL current subscribers.
     * New subscribers that arrive AFTER the stream has completed receive NO items
     * (unlike cache which replays).  Use for broadcasting live events.
     */
    public Flux<String> shareFlux() {
        return expensiveDbQuery().share();
    }

    /**
     * replay(n): creates a ConnectableFlux that keeps the last n items in a buffer.
     * Late subscribers (after connection) still receive the last n items.
     * autoConnect(1) connects when the first subscriber arrives.
     */
    public Flux<String> replayFlux() {
        return expensiveDbQuery()
                .replay(3)
                .autoConnect(1);
    }
}
