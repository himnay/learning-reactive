package com.reactivespring.gateway.filter;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * Validates Bearer JWTs on protected paths and injects X-User-Id into the request
 * so downstream services can trust the caller identity without re-validating the token.
 *
 * Skipped paths (public):
 *  - /actuator/**  (health checks, metrics)
 *  - /fallback/**  (circuit breaker fallbacks)
 *  - /v1/public/** (explicitly public endpoints)
 *
 * Order HIGHEST_PRECEDENCE + 10: runs after RequestIdWebFilter (which is at
 * HIGHEST_PRECEDENCE - 1) but before any route-level filters.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class JwtAuthenticationWebFilter implements WebFilter {

    private static final Logger log = LoggerFactory.getLogger(JwtAuthenticationWebFilter.class);

    @Value("${security.jwt.secret:default-learning-secret-key-min-256-bits-padding1}")
    private String jwtSecret;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();

        if (isPublicPath(path)) {
            return chain.filter(exchange);
        }

        String authHeader = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);

        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            log.warn("Missing or malformed Authorization header for path: {}", path);
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }

        String token = authHeader.substring(7);

        try {
            Claims claims = TokenUtil.validateToken(token, jwtSecret);
            String userId = claims.getSubject();

            // Inject X-User-Id so downstream services don't need to re-validate the JWT.
            ServerWebExchange mutatedExchange = exchange.mutate()
                    .request(exchange.getRequest().mutate()
                            .header("X-User-Id", userId)
                            .build())
                    .build();

            return chain.filter(mutatedExchange);
        } catch (JwtException e) {
            log.warn("Invalid JWT token: {}", e.getMessage());
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }
    }

    private boolean isPublicPath(String path) {
        return path.startsWith("/actuator/")
                || path.startsWith("/fallback/")
                || path.startsWith("/v1/public/");
    }
}
