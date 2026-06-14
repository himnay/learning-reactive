package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class CombineMonoFluxTest {

    private CombineMonoFlux combineMonoFlux = new CombineMonoFlux();

    @Test
    @DisplayName("concat() two flux stream using static method")
    public void concatFluxStreamTest() {
        Flux<String> concatFlux = combineMonoFlux.concatFluxStreamPublisher();

        StepVerifier.create(concatFlux)
                .expectNext("A", "B", "C", "D", "E", "F", "G", "H")
                .verifyComplete();
    }

    @Test
    @DisplayName("concatWith() two flux stream using instance method")
    public void concatWihFluxStreamTest() {
        Flux<String> concatFlux = combineMonoFlux.concatWithFluxStreamPublisher();

        StepVerifier.create(concatFlux)
                .expectNext("A", "B", "C", "D", "E", "F", "G", "H")
                .verifyComplete();
    }

    @Test
    @DisplayName("concatWith() two mono stream")
    public void concatMonoStreamTest() {
        Flux<String> concatFlux = combineMonoFlux.concatWithMonoStreamPublisher();

        StepVerifier.create(concatFlux)
                .expectNext("A", "B")
                .verifyComplete();
    }

    @Test
    @DisplayName("merge() two flux stream using static method")
    public void mergeFluxStreamTest() {
        Flux<String> concatFlux = combineMonoFlux.mergeFluxStreamPublisher();

        StepVerifier.create(concatFlux)
                .expectNext("A", "B", "C", "D", "E", "F", "G", "H")
                .expectError();

        StepVerifier.create(concatFlux)
                .expectNextCount(8)
                .verifyComplete();
    }

    @Test
    @DisplayName("mergeWith() two flux stream using instance method")
    public void mergeWithFluxStreamTest() {
        Flux<String> concatFlux = combineMonoFlux.mergeWithFluxStreamPublisher();

        StepVerifier.create(concatFlux)
                .expectNext("A", "B", "C", "D", "E", "F", "G", "H")
                .expectError();

        StepVerifier.create(concatFlux)
                .expectNextCount(8)
                .verifyComplete();
    }

    @Test
    @DisplayName("mergeWith() two mono stream")
    public void mergeWithMonoStreamTest() {
        Flux<String> concatFlux = combineMonoFlux.mergeWithMonoStreamPublisher();

        StepVerifier.create(concatFlux)
                .expectNext("A", "B")
                .verifyComplete();
    }

    @Test
    @DisplayName("mergeSequential() two flux stream using instance method")
    public void mergeSequentialFluxStreamTest() {
        Flux<String> mergeSequentialFlux = combineMonoFlux.mergeSequentialFluxStreamPublisher();

        StepVerifier.create(mergeSequentialFlux)
                .expectNext("A", "B", "C", "D", "E", "F", "G", "H")
                .expectError();

        StepVerifier.create(mergeSequentialFlux)
                .expectNextCount(8)
                .verifyComplete();
    }

    @Test
    @DisplayName("zip() two flux stream using instance method")
    public void zipFluxStreamTest() {
        Flux<String> zipFlux = combineMonoFlux.zipFluxStreamPublisher();

        StepVerifier.create(zipFlux)
                .expectNext("AE", "BF", "CG", "DH")
                .verifyComplete();
    }

    @Test
    @DisplayName("zip() two flux stream using instance method")
    public void zipMultipleFluxStreamTest() {
        Flux<String> zipFlux = combineMonoFlux.zipMultipleFluxStreamPublisher();

        StepVerifier.create(zipFlux)
                .expectNext("AE15", "BF26", "CG37", "DH48")
                .verifyComplete();
    }

    @Test
    @DisplayName("zipWith() two flux stream using instance method")
    public void zipWithFluxStreamTest() {
        Flux<String> zipWithFlux = combineMonoFlux.zipWithFluxStreamPublisher();

        StepVerifier.create(zipWithFlux)
                .expectNext("AE", "BF", "CG", "DH")
                .verifyComplete();
    }

    @Test
    @DisplayName("zipWith() two mono stream")
    public void zipWithMonoStreamTest() {
        Mono<String> zipWithMono = combineMonoFlux.zipWithMonoStreamPublisher();

        StepVerifier.create(zipWithMono)
                .expectNext("AB")
                .verifyComplete();
    }
}
