package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ParallelOperators")
class ParallelOperatorsTest {

    private final ParallelOperators ops = new ParallelOperators();

    @Test
    @DisplayName("parallelFlux emits 8 squared values and completes")
    void parallelFlux_emitsSquares() {
        StepVerifier.create(ops.parallelFlux().sort())
                .expectNext(1, 4, 9, 16, 25, 36, 49, 64)
                .verifyComplete();
    }

    @Test
    @DisplayName("sequentialMerge emits all doubled values")
    void sequentialMerge_emitsDoubled() {
        List<Integer> results = ops.sequentialMerge().collectList().block();
        assertThat(results).hasSize(8);
        assertThat(results).allMatch(n -> n % 2 == 0);
    }

    @Test
    @DisplayName("parallelVsFlatMap emits 8 squared values")
    void parallelVsFlatMap_emitsSquares() {
        List<Integer> results = ops.parallelVsFlatMap().collectList().block();
        assertThat(results).hasSize(8);
        assertThat(results).containsExactlyInAnyOrder(1, 4, 9, 16, 25, 36, 49, 64);
    }
}
