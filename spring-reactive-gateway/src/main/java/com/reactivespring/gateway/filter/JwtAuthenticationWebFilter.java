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
 * X-User-Id is trusted downstream and keys the rate limiter, so only this filter may set it:
 * any X-User-Id the client sent is dropped first, on public paths too.
 *
 * Order HIGHEST_PRECEDENCE + 10: runs after RequestIdWebFilter (which is at
 * HIGHEST_PRECEDENCE + 1) but before any route-level filters.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class JwtAuthenticationWebFilter implements WebFilter {

    private static final Logger log = LoggerFactory.getLogger(JwtAuthenticationWebFilter.class);

    static final String USER_ID_HEADER = "X-User-Id";

    @Value("${security.jwt.secret:default-learning-secret-key-min-256-bits-padding1}")
    private String jwtSecret;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();

        // Only this filter may set X-User-Id: drop whatever the client sent, public paths included.
        ServerWebExchange stripped = exchange.mutate()
                .request(request -> request.headers(headers -> headers.remove(USER_ID_HEADER)))
                .build();

        if (isPublicPath(path)) {
            return chain.filter(stripped);
        }

        String authHeader = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);

        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            log.warn("Missing or malformed Authorization header for path: {}", path);
            return unauthorized(exchange);
        }

        String token = authHeader.substring(7);

        try {
            Claims claims = TokenUtil.validateToken(token, jwtSecret);
            String userId = claims.getSubject();
            if (userId == null || userId.isBlank()) {
                log.warn("JWT without a subject for path: {}", path);
                return unauthorized(exchange);
            }

            // Inject X-User-Id so downstream services don't need to re-validate the JWT.
            ServerWebExchange mutatedExchange = stripped.mutate()
                    .request(stripped.getRequest().mutate()
                            .header(USER_ID_HEADER, userId)
                            .build())
                    .build();

            return chain.filter(mutatedExchange);
        } catch (JwtException e) {
            log.warn("Invalid JWT token: {}", e.getMessage());
            return unauthorized(exchange);
        }
    }

    private static Mono<Void> unauthorized(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        return exchange.getResponse().setComplete();
    }

    private boolean isPublicPath(String path) {
        return path.startsWith("/actuator/")
                || path.startsWith("/fallback/")
                || path.startsWith("/v1/public/");
    }
}
