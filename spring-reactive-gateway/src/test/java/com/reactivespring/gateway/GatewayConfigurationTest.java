package com.reactivespring.gateway;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.config.GatewayProperties;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.assertj.core.api.Assertions.assertThat;

/** Boots the gateway (no backends, no Redis connection needed) and checks what application.yml wires up. */
@SpringBootTest
@DisplayName("Gateway configuration")
class GatewayConfigurationTest {

    @Autowired
    private GatewayProperties gatewayProperties;

    @Autowired
    private RouteLocator routeLocator;

    @Autowired
    private ApplicationContext context;

    @Test
    @DisplayName("YAML routes and default filters are bound (spring.cloud.gateway.server.webflux.*)")
    void yamlRoutesAndDefaultFiltersAreBound() {
        assertThat(gatewayProperties.getRoutes())
                .extracting(RouteDefinition::getId)
                .containsExactly("movie-info-sse", "movie-review-sse");
        assertThat(gatewayProperties.getDefaultFilters())
                .extracting(FilterDefinition::getName)
                .contains("AddResponseHeader", "SecureHeaders", "RequestRateLimiter");
    }

    @Test
    @DisplayName("Streams take the dedicated SSE routes; other calls take the circuit-breaker routes")
    void streamsTakeTheDedicatedSseRoutes() {
        assertThat(Routes.firstMatch(routeLocator, "/v1/movieInfo/stream")).isEqualTo("movie-info-sse");
        assertThat(Routes.firstMatch(routeLocator, "/v1/reviews/stream")).isEqualTo("movie-review-sse");
        assertThat(Routes.firstMatch(routeLocator, "/v1/movieInfo/42")).isEqualTo("movie-info-service");
        assertThat(Routes.firstMatch(routeLocator, "/v1/reviews")).isEqualTo("movie-review-service");
        assertThat(Routes.firstMatch(routeLocator, "/v1/movies/42")).isEqualTo("movies-service");
    }

    @Test
    @DisplayName("The gateway actuator endpoint is read-only: routes can be listed, not created or refreshed")
    void gatewayActuatorIsReadOnly() {
        WebTestClient client = WebTestClient.bindToApplicationContext(context).build();

        client.get().uri("/actuator/gateway/routes").exchange().expectStatus().isOk();
        client.post().uri("/actuator/gateway/refresh").exchange()
                .expectStatus().value(status -> assertThat(status).isIn(404, 405));
    }

    @Test
    @DisplayName("Circuit-breaker fallbacks answer 503 for any method, not only GET")
    void fallbackAnswersEveryMethod() {
        WebTestClient client = WebTestClient.bindToApplicationContext(context).build();

        client.post().uri("/fallback/movieInfo").exchange().expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        client.delete().uri("/fallback/reviews").exchange().expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        client.get().uri("/fallback/movies").exchange().expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }
}
