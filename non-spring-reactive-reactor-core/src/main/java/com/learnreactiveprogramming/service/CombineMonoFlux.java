package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class CombineMonoFlux {

    // concatenation of streams happens in sequence
    // first one is subscribed first and completes
    // second one is subscribed after that and completes
    public Flux<String> concatFluxStreamPublisher() {
        Flux<String> fluxOne = Flux.just("A", "B", "C", "D");
        Flux<String> fluxTwo = Flux.just("E", "F", "G", "H");

        return Flux.concat(fluxOne, fluxTwo).log();
    }

    // concatWith() flux stream
    public Flux<String> concatWithFluxStreamPublisher() {
        Flux<String> fluxOne = Flux.just("A", "B", "C", "D");
        Flux<String> fluxTwo = Flux.just("E", "F", "G", "H");

        return fluxOne.concatWith(fluxTwo).log();
    }

    // concatWith() mono stream
    public Flux<String> concatWithMonoStreamPublisher() {
        Mono<String> monoOne = Mono.just("A");
        Mono<String> monoTwo = Mono.just("B");

        return monoOne.concatWith(monoTwo).log();
    }

    // merge() flux stream
    public Flux<String> mergeFluxStreamPublisher() {
        Flux<String> fluxOne = Flux.just("A", "B", "C", "D").delayElements(Duration.ofMillis(100));
        Flux<String> fluxTwo = Flux.just("E", "F", "G", "H").delayElements(Duration.ofMillis(120));

        return Flux.merge(fluxOne, fluxTwo).log();
    }

    // mergeWith() flux stream
    public Flux<String> mergeWithFluxStreamPublisher() {
        Flux<String> fluxOne = Flux.just("A", "B", "C", "D").delayElements(Duration.ofMillis(100));
        Flux<String> fluxTwo = Flux.just("E", "F", "G", "H").delayElements(Duration.ofMillis(120));

        return fluxOne.mergeWith(fluxTwo).log();
    }

    // mergeWith() mono stream
    public Flux<String> mergeWithMonoStreamPublisher() {
        Mono<String> monoOne = Mono.just("A");
        Mono<String> monoTwo = Mono.just("B");

        return monoOne.mergeWith(monoTwo).log();
    }

    // mergeSequential() flux stream
    public Flux<String> mergeSequentialFluxStreamPublisher() {
        Flux<String> fluxOne = Flux.just("A", "B", "C", "D").delayElements(Duration.ofMillis(100));
        Flux<String> fluxTwo = Flux.just("E", "F", "G", "H").delayElements(Duration.ofMillis(120));

        return Flux.mergeSequential(fluxOne, fluxTwo).log();
    }

    // zip() flux stream
    public Flux<String> zipFluxStreamPublisher() {
        Flux<String> fluxOne = Flux.just("A", "B", "C", "D");
        Flux<String> fluxTwo = Flux.just("E", "F", "G", "H");

        return Flux.zip(fluxOne, fluxTwo, (a, b) -> a + b).log();
    }

    // zip() flux stream
    public Flux<String> zipMultipleFluxStreamPublisher() {
        Flux<String> fluxOne = Flux.just("A", "B", "C", "D");
        Flux<String> fluxTwo = Flux.just("E", "F", "G", "H");
        Flux<String> fluxThree = Flux.just("1", "2", "3", "4");
        Flux<String> fluxFour = Flux.just("5", "6", "7", "8");

        return Flux.zip(fluxOne, fluxTwo, fluxThree, fluxFour)
                .map(t4 -> t4.getT1() + t4.getT2() + t4.getT3() + t4.getT4())
                .log();
    }

    // zipWith() flux stream
    public Flux<String> zipWithFluxStreamPublisher() {
        Flux<String> fluxOne = Flux.just("A", "B", "C", "D");
        Flux<String> fluxTwo = Flux.just("E", "F", "G", "H");

        return fluxOne.zipWith(fluxTwo, (a, b) -> a + b).log();
    }

    // zipWith() mono stream
    public Mono<String> zipWithMonoStreamPublisher() {
        Mono<String> monoOne = Mono.just("A");
        Mono<String> monoTwo = Mono.just("B");

        return monoOne.zipWith(monoTwo, (a, b) -> a + b).log();
    }


}