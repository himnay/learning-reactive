package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * True parallel processing with Reactor's ParallelFlux.
 * Unlike flatMap concurrency (which uses async I/O on one thread),
 * ParallelFlux distributes work across multiple CPU cores.
 */
public class ParallelOperators {

    /**
     * parallel(4): splits the upstream into 4 "rails" (partitions).
     * runOn(parallel()): assigns each rail to a thread from the parallel scheduler.
     * This achieves true CPU-level parallelism for CPU-bound work.
     */
    public Flux<Integer> parallelFlux() {
        return Flux.range(1, 8)
                .parallel(4)
                .runOn(Schedulers.parallel())
                .map(n -> {
                    System.out.println("Processing " + n + " on " + Thread.currentThread().getName());
                    return n * n;
                })
                .sequential(); // merge rails back into a single Flux
    }

    /**
     * .sequential() merges the parallel rails back into a single ordered Flux.
     * Items from different rails are interleaved by arrival time, not original order.
     * Use when you need a Flux<T> consumer after parallel processing.
     */
    public Flux<Integer> sequentialMerge() {
        return Flux.range(1, 8)
                .parallel(4)
                .runOn(Schedulers.parallel())
                .map(n -> n * 2)
                .sequential();
    }

    /**
     * flatMap with concurrency parameter is an alternative to parallel() for I/O-bound work.
     * It does NOT use multiple threads — it creates up to N concurrent subscriptions
     * on the SAME thread pool.  Good for reactive HTTP calls, not CPU-bound computation.
     *
     * parallel() is better for CPU work; flatMap(fn, concurrency) is better for async I/O.
     */
    public Flux<Integer> parallelVsFlatMap() {
        // flatMap concurrency=4: up to 4 inner Monos active simultaneously
        return Flux.range(1, 8)
                .flatMap(n -> Flux.just(n * n).subscribeOn(Schedulers.boundedElastic()), 4);
    }
}
