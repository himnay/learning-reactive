package com.reactivespring.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.ratelimit.RedisRateLimiter;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;

/**
 * Programmatic route definition (RouteLocator DSL).
 * Main REST API routes with circuit breakers, retries, bulkhead, and rate limiting.
 */
@Configuration
public class GatewayRoutesConfig {

    @Value("${services.info-url:http://localhost:8080}")
    private String infoServiceUrl;

    @Value("${services.review-url:http://localhost:8081}")
    private String reviewServiceUrl;

    @Value("${services.movies-url:http://localhost:8082}")
    private String moviesServiceUrl;

    @Bean
    public RedisRateLimiter redisRateLimiter() {
        // replenishRate: tokens added per second; burstCapacity: max tokens in bucket;
        // requestedTokens: tokens consumed per request (1 = standard)
        return new RedisRateLimiter(10, 20, 1);
    }

    @Bean
    public RouteLocator gatewayRouteLocator(RouteLocatorBuilder builder, KeyResolver userKeyResolver) {
        return builder.routes()

            // ── Movie Info Service ─────────────────────────────────────────────
            .route("movie-info-service", r -> r
                .path("/v1/movieInfo/**")
                .filters(f -> f
                    .addRequestHeader("X-Gateway-Source", "spring-reactive-gateway")
                    .circuitBreaker(c -> c
                        .setName("movieInfoCB")
                        .setFallbackUri("forward:/fallback/movieInfo"))
                    .retry(config -> config
                        .setRetries(3)
                        .setStatuses(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.SERVICE_UNAVAILABLE))
                    // Bulkhead: max 10 concurrent calls; new calls are rejected immediately
                    // if the semaphore is full (maxWaitDuration=0)
                    .requestRateLimiter(config -> config
                        .setRateLimiter(redisRateLimiter())
                        .setKeyResolver(userKeyResolver)))
                .uri(infoServiceUrl))

            // ── Movie Review Service ───────────────────────────────────────────
            .route("movie-review-service", r -> r
                .path("/v1/reviews/**")
                .filters(f -> f
                    .addRequestHeader("X-Gateway-Source", "spring-reactive-gateway")
                    .circuitBreaker(c -> c
                        .setName("reviewsCB")
                        .setFallbackUri("forward:/fallback/reviews"))
                    .retry(config -> config
                        .setRetries(3)
                        .setStatuses(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.SERVICE_UNAVAILABLE)))
                .uri(reviewServiceUrl))

            // ── Movies Aggregation Service ─────────────────────────────────────
            .route("movies-service", r -> r
                .path("/v1/movies/**")
                .filters(f -> f
                    .addRequestHeader("X-Gateway-Source", "spring-reactive-gateway")
                    .circuitBreaker(c -> c
                        .setName("moviesCB")
                        .setFallbackUri("forward:/fallback/movies")))
                .uri(moviesServiceUrl))

            .build();
    }
}
