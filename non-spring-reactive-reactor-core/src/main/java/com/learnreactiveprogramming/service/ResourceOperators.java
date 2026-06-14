package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

/**
 * Resource lifecycle operators that guarantee cleanup regardless of how a stream ends
 * (complete, error, or cancel).
 */
public class ResourceOperators {

    // Fake "connection" resource for demo purposes
    record FakeConnection(String id) {
        static Mono<FakeConnection> open() {
            System.out.println("Opening connection");
            return Mono.just(new FakeConnection("conn-" + System.nanoTime()));
        }

        Mono<Void> close() {
            System.out.println("Closing connection: " + id);
            return Mono.empty();
        }

        Mono<String> query(String sql) {
            return Mono.just("result of: " + sql + " on " + id);
        }
    }

    /**
     * doFinally: runs a cleanup action on complete, error, AND cancel.
     * Unlike doOnTerminate (complete/error only), doFinally also fires on cancel —
     * critical for releasing database connections, file handles, etc.
     */
    public Flux<String> doFinally() {
        return Flux.just("a", "b", "c")
                .doFinally(signalType -> {
                    // SignalType.ON_COMPLETE, ON_ERROR, or CANCEL
                    System.out.println("Stream ended via: " + signalType);
                    // Release external resource here
                });
    }

    /**
     * usingWhen: reactive resource lifecycle.
     * - First arg: Mono that acquires the resource reactively.
     * - Second arg: function that uses the resource.
     * - Third arg: async release on success.
     * - Fourth arg: async release on error (with the error).
     * - Fifth arg: async release on cancel.
     * The resource is always released, even if the consumer errors or is cancelled.
     */
    public Mono<String> usingWhen() {
        return Mono.usingWhen(
                FakeConnection.open(),                          // acquire
                conn -> conn.query("SELECT 1"),                 // use
                FakeConnection::close,                         // release on success
                (conn, err) -> conn.close(),                   // release on error
                FakeConnection::close                          // release on cancel
        );
    }

    /**
     * using: synchronous resource lifecycle (non-reactive acquire and release).
     * Use when the resource acquisition itself is not asynchronous (e.g., opening
     * a file, acquiring a lock) but you still want guaranteed release.
     */
    public Flux<String> using() {
        return Flux.using(
                () -> {
                    System.out.println("Sync resource acquired");
                    return "resource-handle";
                },
                handle -> Flux.just("item-from-" + handle, "item2-from-" + handle),
                handle -> System.out.println("Sync resource released: " + handle)
        );
    }
}
