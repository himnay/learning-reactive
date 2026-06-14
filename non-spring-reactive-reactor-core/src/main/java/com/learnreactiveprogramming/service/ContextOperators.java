package com.learnreactiveprogramming.service;

import org.slf4j.MDC;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

/**
 * Reactor Context is an immutable key-value store attached to each subscription.
 * It travels UPSTREAM (from subscriber to publisher) and is read DOWNSTREAM via
 * deferContextual.  It is the reactive replacement for ThreadLocal / MDC.
 *
 * Key insight: context is written with contextWrite (downstream) and read with
 * Mono.deferContextual (anywhere in the chain above the contextWrite).
 */
public class ContextOperators {

    /**
     * Writing a value into context with contextWrite, then reading it with
     * deferContextual.  Context flows upstream, so contextWrite below the reader
     * still makes the value visible to the reader.
     */
    public Mono<String> basicContextWrite() {
        return Mono.deferContextual(ctx -> {
                    String userId = ctx.getOrDefault("userId", "anonymous");
                    return Mono.just("Hello, " + userId);
                })
                // contextWrite applies below the chain; context still propagates upstream
                .contextWrite(ctx -> ctx.put("userId", "user-42"));
    }

    /**
     * Simulates passing a userId through a reactive pipeline without a ThreadLocal.
     * Each stage reads the userId from context rather than from a method parameter.
     */
    public Mono<String> contextPropagation() {
        return fetchUserData()
                .flatMap(data -> Mono.deferContextual(ctx -> {
                    String userId = ctx.getOrDefault("userId", "unknown");
                    return Mono.just(data + " [processed for " + userId + "]");
                }))
                .contextWrite(Context.of("userId", "user-99"));
    }

    private Mono<String> fetchUserData() {
        return Mono.deferContextual(ctx ->
                Mono.just("data-for-" + ctx.getOrDefault("userId", "nobody")));
    }

    /**
     * Shows how to propagate a correlationId from context into MDC for logging.
     * MDC is thread-local, so it cannot be set once and trusted across thread hops.
     * The correct pattern: read from context, set MDC, log, clear MDC — all in one
     * synchronous block before any thread switch.
     */
    public Mono<String> mdc_correlation() {
        return Mono.deferContextual(ctx -> {
                    String correlationId = ctx.getOrDefault("correlationId", "no-correlation");
                    // Set MDC only for the duration of this synchronous call
                    MDC.put("correlationId", correlationId);
                    try {
                        // Logger would pick up correlationId from MDC here
                        System.out.println("[" + correlationId + "] Processing on " + Thread.currentThread().getName());
                        return Mono.just("processed-" + correlationId);
                    } finally {
                        MDC.remove("correlationId");
                    }
                })
                .contextWrite(ctx -> ctx.put("correlationId", "req-abc-123"));
    }

    /**
     * Inner context overrides outer context only for the nested pipeline.
     * The outer pipeline remains unaffected — contexts are immutable and scoped.
     */
    public Mono<String> nestedContext() {
        return Mono.deferContextual(outerCtx -> {
                    String outer = outerCtx.getOrDefault("scope", "none");
                    // Inner pipeline with a different context value
                    Mono<String> inner = Mono.deferContextual(innerCtx ->
                                    Mono.just("inner-scope=" + innerCtx.getOrDefault("scope", "none")))
                            .contextWrite(ctx -> ctx.put("scope", "inner"));
                    return inner.map(v -> "outer-scope=" + outer + ", " + v);
                })
                .contextWrite(ctx -> ctx.put("scope", "outer"));
    }
}
