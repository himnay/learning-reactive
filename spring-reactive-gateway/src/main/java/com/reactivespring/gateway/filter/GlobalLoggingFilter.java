package com.reactivespring.gateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * GlobalFilter that:
 *  1. Generates / propagates a correlation ID on every request
 *  2. Logs inbound method + path before forwarding
 *  3. Logs status code + elapsed time after the downstream response returns
 */
@Component
public class GlobalLoggingFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(GlobalLoggingFilter.class);
    static final String CORRELATION_ID_HEADER = "X-Correlation-Id";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String correlationId = exchange.getRequest().getHeaders().getFirst(CORRELATION_ID_HEADER);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        final String cid = correlationId;
        final long start = System.currentTimeMillis();

        log.info("→ {} {} correlationId={}", exchange.getRequest().getMethod(),
                exchange.getRequest().getURI().getPath(), cid);

        ServerHttpRequest mutatedRequest = exchange.getRequest().mutate()
                .header(CORRELATION_ID_HEADER, cid)
                .build();

        return chain.filter(exchange.mutate().request(mutatedRequest).build())
                .doOnSuccess(v -> {
                    exchange.getResponse().getHeaders().add(CORRELATION_ID_HEADER, cid);
                    log.info("← {} {} {}ms correlationId={}",
                            exchange.getResponse().getStatusCode(),
                            exchange.getRequest().getURI().getPath(),
                            System.currentTimeMillis() - start, cid);
                });
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
