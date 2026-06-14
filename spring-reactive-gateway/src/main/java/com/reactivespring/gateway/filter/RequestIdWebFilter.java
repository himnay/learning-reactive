package com.reactivespring.gateway.filter;

import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.UUID;

/**
 * Ensures every inbound request carries an X-Request-Id header.
 * If the caller already provided one, we honour it (idempotent for distributed tracing).
 * If not, we mint a new UUID.  The id is also written into Reactor Context so
 * downstream operators can read it without re-parsing headers.
 *
 * Order is HIGHEST_PRECEDENCE - 1 so this runs before almost everything else
 * (HIGHEST_PRECEDENCE itself is reserved for security/auth filters).
 */
@Component
@org.springframework.core.annotation.Order(Ordered.HIGHEST_PRECEDENCE - 1)
public class RequestIdWebFilter implements WebFilter {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String requestId = Optional.ofNullable(
                        exchange.getRequest().getHeaders().getFirst("X-Request-Id"))
                .orElse(UUID.randomUUID().toString());

        return chain.filter(exchange.mutate()
                        .request(exchange.getRequest().mutate()
                                .header("X-Request-Id", requestId)
                                .build())
                        .build())
                .contextWrite(ctx -> ctx.put("requestId", requestId));
    }
}
