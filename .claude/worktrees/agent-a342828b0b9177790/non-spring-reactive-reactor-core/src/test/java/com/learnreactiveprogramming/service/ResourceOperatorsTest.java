package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

@DisplayName("ResourceOperators")
class ResourceOperatorsTest {

    private final ResourceOperators ops = new ResourceOperators();

    @Test
    @DisplayName("doFinally emits items and runs cleanup on completion")
    void doFinally_runsCleanupOnComplete() {
        StepVerifier.create(ops.doFinally())
                .expectNext("a", "b", "c")
                .verifyComplete();
        // Cleanup should have printed "Stream ended via: onComplete"
    }

    @Test
    @DisplayName("usingWhen acquires connection, queries, and releases it")
    void usingWhen_acquiresAndReleasesConnection() {
        StepVerifier.create(ops.usingWhen())
                .expectNextMatches(result -> result.startsWith("result of: SELECT 1"))
                .verifyComplete();
    }

    @Test
    @DisplayName("using synchronous resource is acquired and released")
    void using_acquiresAndReleasesSync() {
        StepVerifier.create(ops.using())
                .expectNext("item-from-resource-handle", "item2-from-resource-handle")
                .verifyComplete();
    }
}
