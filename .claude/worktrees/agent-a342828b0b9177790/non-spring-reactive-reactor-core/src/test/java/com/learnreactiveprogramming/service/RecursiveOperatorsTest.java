package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("RecursiveOperators")
class RecursiveOperatorsTest {

    private final RecursiveOperators ops = new RecursiveOperators();

    @Test
    @DisplayName("expandBreadthFirst traverses tree in BFS order")
    void expandBreadthFirst_bfsOrder() {
        List<Integer> result = ops.expandBreadthFirst().collectList().block();
        // BFS: 1, 2, 3, 4, 5, 6, 7
        assertThat(result).containsExactly(1, 2, 3, 4, 5, 6, 7);
    }

    @Test
    @DisplayName("expandDeepFirst traverses tree in DFS order")
    void expandDeepFirst_dfsOrder() {
        List<Integer> result = ops.expandDeepFirst().collectList().block();
        // DFS pre-order: 1, 2, 4, 5, 3, 6, 7
        assertThat(result).containsExactly(1, 2, 4, 5, 3, 6, 7);
    }

    @Test
    @DisplayName("directoryTraversal visits all nodes")
    void directoryTraversal_visitsAllNodes() {
        StepVerifier.create(ops.directoryTraversal())
                .expectNextMatches(p -> p.equals("/root"))
                .expectNextCount(2) // /root/a and /root/b in BFS
                .verifyComplete();
    }
}
