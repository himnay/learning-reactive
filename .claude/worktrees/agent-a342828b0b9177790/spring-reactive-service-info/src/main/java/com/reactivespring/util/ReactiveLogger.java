package com.reactivespring.util;

import org.slf4j.MDC;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Utility for propagating MDC correlation data in reactive pipelines.
 *
 * ThreadLocal (and thus MDC) cannot be trusted across thread hops in a reactive stream.
 * The correct pattern is to store correlation data in Reactor Context, then read it
 * into MDC synchronously inside a deferContextual block immediately before logging.
 */
public final class ReactiveLogger {

    private ReactiveLogger() {}

    /**
     * Wraps a log action with MDC population from Reactor Context.
     * The MDC key/value are set and cleared synchronously within this call —
     * never leaking to a different thread or subscription.
     */
    public static <T> Consumer<T> withMdc(String contextKey, Consumer<T> logAction) {
        return item -> Mono.deferContextual(ctx -> {
                    String value = ctx.getOrDefault(contextKey, "unknown");
                    MDC.put(contextKey, value);
                    try {
                        logAction.accept(item);
                    } finally {
                        // Always clear to avoid polluting thread pool thread for the next task
                        MDC.remove(contextKey);
                    }
                    return Mono.empty();
                }).subscribe();
    }

    /**
     * Adds correlationId to the Reactor Context so it is available to all upstream
     * operators that call withMdc().  Call this at the outermost layer (e.g., a WebFilter
     * that reads X-Correlation-Id from the request header).
     */
    public static <T> Function<Mono<T>, Mono<T>> withCorrelationId(String correlationId) {
        return mono -> mono.contextWrite(ctx -> ctx.put("correlationId", correlationId));
    }
}
