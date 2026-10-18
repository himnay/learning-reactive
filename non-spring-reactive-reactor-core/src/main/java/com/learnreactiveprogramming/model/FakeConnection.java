package com.learnreactiveprogramming.model;

import reactor.core.publisher.Mono;

// Fake "connection" resource for demo purposes
public record FakeConnection(String id) {
    public static Mono<FakeConnection> open() {
        System.out.println("Opening connection");
        return Mono.just(new FakeConnection("conn-" + System.nanoTime()));
    }

    public Mono<Void> close() {
        System.out.println("Closing connection: " + id);
        return Mono.empty();
    }

    public Mono<String> query(String sql) {
        return Mono.just("result of: " + sql + " on " + id);
    }
}
