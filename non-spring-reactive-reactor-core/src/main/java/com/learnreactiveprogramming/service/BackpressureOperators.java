package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

/**
 * Backpressure: the mechanism by which a slow subscriber signals a fast producer
 * to slow down.  When the subscriber cannot keep up, the operator must decide what
 * to do with the excess items: buffer, drop, or keep only the latest.
 */
public class BackpressureOperators {

    /**
     * Buffer overflow: items accumulate in a bounded buffer.  When the buffer is full,
     * DROP_OLDEST discards the oldest buffered item to make room for the new one.
     * Prevents unbounded memory growth while preserving recent data.
     */
    public Flux<Integer> onBackpressureBuffer() {
        return Flux.range(1, 100)
                .onBackpressureBuffer(10, dropped ->
                        System.out.println("Dropped oldest: " + dropped),
                        reactor.core.publisher.BufferOverflowStrategy.DROP_OLDEST)
                .publishOn(Schedulers.boundedElastic());
    }

    /**
     * DROP: items that arrive when the subscriber is busy are silently discarded.
     * Best for metrics/telemetry where losing a sample is acceptable.
     */
    public Flux<Integer> onBackpressureDrop() {
        return Flux.<Integer>create(sink -> {
            for (int i = 1; i <= 100; i++) {
                sink.next(i);
            }
            sink.complete();
        }, FluxSink.OverflowStrategy.DROP);
    }

    /**
     * LATEST: only the most recent unseen item is kept when subscriber is slow.
     * Earlier pending items are replaced by the newer one.
     * Good for UI state updates where only the freshest value matters.
     */
    public Flux<Integer> onBackpressureLatest() {
        return Flux.range(1, 100)
                .onBackpressureLatest();
    }

    /**
     * limitRate controls the prefetch size: the subscriber requests items in
     * batches of N rather than Long.MAX_VALUE at once.
     * This prevents the producer from overwhelming the pipeline.
     */
    public Flux<Integer> limitRate() {
        return Flux.range(1, 100)
                .doOnRequest(n -> System.out.println("Requested: " + n))
                .limitRate(10);
    }

    /**
     * take(n) cancels the upstream after n elements, effectively limiting total demand.
     * Semantically different from limitRate: take() terminates; limitRate() throttles.
     */
    public Flux<Integer> limitRequest() {
        return Flux.range(1, 100)
                .take(5);
    }
}
