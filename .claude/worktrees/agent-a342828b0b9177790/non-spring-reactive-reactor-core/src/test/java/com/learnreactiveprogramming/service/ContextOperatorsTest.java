package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

@DisplayName("ContextOperators")
class ContextOperatorsTest {

    private final ContextOperators ops = new ContextOperators();

    @Test
    @DisplayName("basicContextWrite reads userId from context")
    void basicContextWrite_readsUserId() {
        StepVerifier.create(ops.basicContextWrite())
                .expectNext("Hello, user-42")
                .verifyComplete();
    }

    @Test
    @DisplayName("contextPropagation propagates userId through the pipeline")
    void contextPropagation_propagatesUserId() {
        StepVerifier.create(ops.contextPropagation())
                .expectNextMatches(s -> s.contains("user-99"))
                .verifyComplete();
    }

    @Test
    @DisplayName("mdc_correlation reads correlationId from context")
    void mdc_correlation_readsCorrelationId() {
        StepVerifier.create(ops.mdc_correlation())
                .expectNextMatches(s -> s.startsWith("processed-req-abc-123"))
                .verifyComplete();
    }

    @Test
    @DisplayName("nestedContext inner scope overrides outer for nested pipeline only")
    void nestedContext_innerOverridesOuter() {
        StepVerifier.create(ops.nestedContext())
                .expectNextMatches(s -> s.contains("outer-scope=outer") && s.contains("inner-scope=inner"))
                .verifyComplete();
    }
}
