package com.learnreactiveprogramming.service;

import reactor.core.publisher.Mono;

import java.util.List;

public class MonoFlow {

    // event using mono just() for a single event
    /** Returns the mono publisher. */
    public Mono<String> monoPublisher() {
        return Mono.just("******Alex******").log();
    }

    // list of mono events using just()
    /** Returns the mono list publisher. */
    public Mono<List<String>> monoListPublisher() {
        return Mono.just(List.of("Himansu".split("")));
    }

}
