package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Batching, grouping, and aggregation operators for processing streams in chunks
 * or reducing them to summary values.
 */
public class BatchingOperators {

    /**
     * buffer(n): collects upstream items into Lists of size n, then emits each List.
     * Useful for micro-batching DB writes or network calls.
     */
    public Flux<List<Integer>> bufferByCount() {
        return Flux.range(1, 10).buffer(3);
    }

    /**
     * buffer(Duration): collects all items arriving within a time window into one List.
     * In tests, use StepVerifier.withVirtualTime to avoid real sleeping.
     */
    public Flux<List<Long>> bufferByTime() {
        return Flux.interval(Duration.ofMillis(100))
                .take(9)
                .buffer(Duration.ofMillis(500));
    }

    /**
     * window(n): like buffer, but emits Flux<T> inner publishers instead of List<T>.
     * The inner Flux is lazy — items flow through on demand rather than being collected.
     * Use when downstream processing of each group should itself be reactive.
     */
    public Flux<Flux<Integer>> windowByCount() {
        return Flux.range(1, 10).window(3);
    }

    /**
     * groupBy: partitions items into keyed sub-streams (GroupedFlux).
     * Each group is a hot publisher; consuming it must happen inside flatMap/concatMap
     * to avoid memory leaks from unsubscribed inner publishers.
     */
    public Flux<String> groupBy() {
        return Flux.just("apple", "avocado", "banana", "blueberry", "cherry")
                .groupBy(s -> s.charAt(0))
                .flatMap(group -> group
                        .collectList()
                        .map(list -> group.key() + ": " + list));
    }

    /**
     * scan: running (incremental) aggregation — emits a value after each item.
     * The initial accumulator (0) is emitted first.
     * Result: 0, 1, 3, 6, 10, 15
     */
    public Flux<Integer> scan() {
        return Flux.range(1, 5).scan(0, Integer::sum);
    }

    /**
     * reduce: terminal aggregation — emits one Mono with the final accumulated value.
     * Unlike scan, intermediate sums are not emitted.
     */
    public Mono<Integer> reduce() {
        return Flux.range(1, 5).reduce(0, Integer::sum);
    }

    /**
     * collectMap: collects all items into a Map using a key extractor.
     * Key collision: later items overwrite earlier ones by default.
     */
    public Mono<Map<Integer, String>> collectMap() {
        return Flux.just("a", "bb", "ccc")
                .collectMap(String::length);
    }
}
