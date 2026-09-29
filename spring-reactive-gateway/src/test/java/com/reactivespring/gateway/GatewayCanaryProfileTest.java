package com.reactivespring.gateway;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.config.GatewayProperties;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@SpringBootTest
@ActiveProfiles({"local", "canary"})
@DisplayName("Gateway canary profile")
class GatewayCanaryProfileTest {

    @Autowired
    private GatewayProperties gatewayProperties;

    @Autowired
    private RouteLocator routeLocator;

    @Test
    @DisplayName("Weighted routes come ahead of the programmatic route and keep its circuit breaker and retry")
    void weightedRoutesCarryTheResilienceFilters() {
        assertThat(gatewayProperties.getRoutes())
                .extracting(RouteDefinition::getId, RouteDefinition::getOrder)
                .containsExactly(
                        tuple("movie-info-sse", -2),
                        tuple("movie-review-sse", -2),
                        tuple("movie-info-canary-v2", -1),
                        tuple("movie-info-stable", -1));
        assertThat(gatewayProperties.getRoutes())
                .filteredOn(route -> route.getId().startsWith("movie-info-") && !route.getId().endsWith("-sse"))
                .allSatisfy(route -> assertThat(route.getFilters())
                        .extracting(FilterDefinition::getName)
                        .contains("CircuitBreaker", "Retry"));
    }

    @Test
    @DisplayName("Streams still take the SSE route with the canary on")
    void streamsStillTakeTheSseRoute() {
        assertThat(Routes.firstMatch(routeLocator, "/v1/movieInfo/stream")).isEqualTo("movie-info-sse");
    }
}
