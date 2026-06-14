package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

/**
 * Demonstrates the most misunderstood Reactor scheduling concept:
 * - subscribeOn: affects which thread SUBSCRIBES (and thus executes the source),
 *   regardless of where it is placed in the chain.
 * - publishOn: affects which thread executes all operators DOWNSTREAM of its position.
 */
public class SchedulerOperators {

    /**
     * subscribeOn(boundedElastic) moves the subscription (and source execution) onto
     * a thread from the bounded elastic pool — designed for blocking I/O calls so
     * that the event loop is never blocked.
     */
    public Mono<String> subscribeOnBoundedElastic() {
        return Mono.fromCallable(() -> {
                    System.out.println("Source on: " + Thread.currentThread().getName());
                    // Simulates a blocking call (e.g., legacy JDBC, file I/O)
                    Thread.sleep(100);
                    return "result";
                })
                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(v -> System.out.println("After subscribeOn on: " + Thread.currentThread().getName()));
    }

    /**
     * publishOn switches the execution of downstream operators to a parallel scheduler.
     * The source still runs on whatever thread subscribes; only the operators after
     * publishOn move to the new scheduler.
     */
    public Flux<Integer> publishOnParallel() {
        return Flux.range(1, 5)
                .doOnNext(i -> System.out.println("Before publishOn on: " + Thread.currentThread().getName()))
                .publishOn(Schedulers.parallel())
                .doOnNext(i -> System.out.println("After publishOn on: " + Thread.currentThread().getName()))
                .map(i -> i * 2);
    }

    /**
     * Key rule: subscribeOn affects where the SOURCE runs, no matter where it appears.
     * publishOn affects operators placed AFTER it in the chain.
     * Here we see both used together to demonstrate the split.
     */
    public Flux<String> subscribeOnVsPublishOn() {
        return Flux.just("a", "b", "c")
                // subscribeOn placed mid-chain; still moves the SOURCE to boundedElastic
                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(s -> System.out.println("Post-subscribeOn (still boundedElastic): " + Thread.currentThread().getName()))
                // publishOn from this point moves remaining operators to parallel
                .publishOn(Schedulers.parallel())
                .doOnNext(s -> System.out.println("Post-publishOn (parallel): " + Thread.currentThread().getName()))
                .map(String::toUpperCase);
    }

    /**
     * Canonical pattern for wrapping a blocking legacy API call reactively.
     * Mono.fromCallable defers execution until subscription; subscribeOn ensures
     * the blocking code never runs on the Netty event loop.
     */
    public Mono<String> wrapBlockingCall() {
        return Mono.fromCallable(() -> {
                    // Legacy blocking call (DB driver, file read, etc.)
                    Thread.sleep(50);
                    return "legacy-result-" + Thread.currentThread().getName();
                })
                .subscribeOn(Schedulers.boundedElastic());
    }
}
