package com.learnreactiveprogramming.service;

import reactor.core.publisher.Mono;

import java.util.List;

public class MonoFlow {

    // event using mono just() for a single event
    public Mono<String> monoPublisher() {
        return Mono.just("******Alex******").log();
    }

    // list of mono events using just()
    public Mono<List<String>> monoListPublisher() {
        return Mono.just(List.of("Himansu".split("")));
    }

}
