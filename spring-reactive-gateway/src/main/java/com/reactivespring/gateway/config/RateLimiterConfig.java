package com.reactivespring.gateway.config;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.util.Objects;

@Configuration
public class RateLimiterConfig {

    /**
     * Rate-limit key resolver: prefer X-User-Id (set by JwtAuthenticationWebFilter)
     * so authenticated users share a per-user bucket.  Fall back to remote IP for
     * unauthenticated traffic (e.g., public endpoints that bypass JWT validation).
     */
    @Bean
    public KeyResolver userKeyResolver() {
        return exchange -> Mono.justOrEmpty(
                exchange.getRequest().getHeaders().getFirst("X-User-Id"))
                .switchIfEmpty(Mono.just(
                        Objects.requireNonNull(exchange.getRequest().getRemoteAddress())
                                .getAddress().getHostAddress()));
    }
}
