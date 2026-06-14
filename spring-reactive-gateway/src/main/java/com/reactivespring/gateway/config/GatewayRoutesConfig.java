package com.reactivespring.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Programmatic route definition (RouteLocator DSL) — preferred over YAML routes for
 * type-safety and IDE support.  Each downstream service is addressable via its
 * base URL so the gateway remains environment-independent (dev/test/prod can
 * override via application.yml or environment variable).
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
    public RouteLocator gatewayRouteLocator(RouteLocatorBuilder builder) {
        return builder.routes()

            // ── Movie Info Service ───────────────────────────────────────────────
            .route("movie-info-service", r -> r
                .path("/v1/movieInfo/**")
                .filters(f -> f
                    .addRequestHeader("X-Gateway-Source", "spring-reactive-gateway")
                    .circuitBreaker(c -> c
                        .setName("movieInfoCB")
                        .setFallbackUri("forward:/fallback/movieInfo"))
                    .retry(config -> config
                        .setRetries(3)
                        .setStatuses(
                            org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR,
                            org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE)))
                .uri(infoServiceUrl))

            // ── Movie Review Service ─────────────────────────────────────────────
            .route("movie-review-service", r -> r
                .path("/v1/reviews/**")
                .filters(f -> f
                    .addRequestHeader("X-Gateway-Source", "spring-reactive-gateway")
                    .circuitBreaker(c -> c
                        .setName("reviewsCB")
                        .setFallbackUri("forward:/fallback/reviews"))
                    .retry(config -> config
                        .setRetries(3)
                        .setStatuses(
                            org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR,
                            org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE)))
                .uri(reviewServiceUrl))

            // ── Movies Aggregation Service ───────────────────────────────────────
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
