package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Demonstrates lazy vs. eager evaluation in Reactor.
 * Mono.just(expr) evaluates expr IMMEDIATELY at assembly time — surprising for
 * expensive or time-sensitive values.  Mono.defer / Mono.fromCallable evaluate
 * the supplier at SUBSCRIPTION time, giving each subscriber its own fresh value.
 */
public class DeferOperators {

    private int callCount = 0;

    private String expensiveCall() {
        callCount++;
        System.out.println("expensiveCall executed, count=" + callCount);
        return "value-" + callCount;
    }

    /**
     * EAGER: expensiveCall() runs once at assembly, both subscribers see "value-1".
     * LAZY: with defer, each subscription triggers a fresh call.
     */
    public Mono<String> monoDefer() {
        // Without defer — call happens NOW
        // Mono<String> eager = Mono.just(expensiveCall());

        // With defer — call deferred to subscription
        return Mono.defer(() -> Mono.just(expensiveCall()));
    }

    /**
     * Flux.defer wraps the entire Flux creation in a factory lambda so the
     * publisher is created fresh for each subscriber.
     */
    public Flux<Integer> fluxDefer() {
        return Flux.defer(() -> {
            System.out.println("Flux created for subscriber on: " + Thread.currentThread().getName());
            return Flux.range(1, 3);
        });
    }

    /**
     * Mono.fromCallable accepts a Callable that may throw a checked exception.
     * The exception is caught and turned into an onError signal — safer than
     * wrapping in try/catch inside Mono.just().
     */
    public Mono<String> fromCallable() {
        return Mono.fromCallable(() -> {
            // Could throw IOException, SQLExcepton, etc.
            if (Math.random() < 0) throw new Exception("checked!");
            return "from-callable";
        });
    }

    /**
     * Mono.fromSupplier accepts an unchecked Supplier — syntactically cleaner
     * when no checked exceptions are involved.
     */
    public Mono<String> fromSupplier() {
        return Mono.fromSupplier(() -> "from-supplier-" + System.nanoTime());
    }

    /**
     * Classic "Mono.just captures assembly time" gotcha:
     * - eagerTime: Instant.now() called once; every subscriber gets the SAME timestamp.
     * - lazyTime: each subscriber gets their own Instant.now() at subscribe time.
     */
    public Mono<Instant> deferredCurrentTime() {
        // Mono<Instant> eager = Mono.just(Instant.now());  // same for all
        return Mono.defer(() -> Mono.just(Instant.now()));
    }
}
