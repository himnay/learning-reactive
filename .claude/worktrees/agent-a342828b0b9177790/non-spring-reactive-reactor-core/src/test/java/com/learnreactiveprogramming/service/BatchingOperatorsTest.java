package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@DisplayName("BatchingOperators")
class BatchingOperatorsTest {

    private final BatchingOperators ops = new BatchingOperators();

    @Test
    @DisplayName("bufferByCount groups items into lists of size 3")
    void bufferByCount_groupsInThrees() {
        StepVerifier.create(ops.bufferByCount())
                .expectNext(List.of(1, 2, 3))
                .expectNext(List.of(4, 5, 6))
                .expectNext(List.of(7, 8, 9))
                .expectNext(List.of(10))
                .verifyComplete();
    }

    @Test
    @DisplayName("scan emits running sum starting from 0")
    void scan_runningSum() {
        StepVerifier.create(ops.scan())
                .expectNext(0, 1, 3, 6, 10, 15)
                .verifyComplete();
    }

    @Test
    @DisplayName("reduce produces terminal sum 15")
    void reduce_terminalSum() {
        StepVerifier.create(ops.reduce())
                .expectNext(15)
                .verifyComplete();
    }

    @Test
    @DisplayName("collectMap produces map from string length to string")
    void collectMap_producesMap() {
        StepVerifier.create(ops.collectMap())
                .expectNextMatches(map ->
                        map.get(1).equals("a") &&
                        map.get(2).equals("bb") &&
                        map.get(3).equals("ccc"))
                .verifyComplete();
    }

    @Test
    @DisplayName("groupBy groups strings by first character")
    void groupBy_groupsByFirstChar() {
        StepVerifier.create(ops.groupBy())
                .expectNextCount(3) // 3 unique first characters: a, b, c
                .verifyComplete();
    }

    @Test
    @DisplayName("bufferByTime emits lists within 500ms window using virtual time")
    void bufferByTime_virtualTime() {
        StepVerifier.withVirtualTime(ops::bufferByTime)
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(2))
                .expectNextCount(2) // 10 items at 100ms each = ~2 windows of 500ms
                .verifyComplete();
    }
}
