package com.reactivespring.gateway;

import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Duration;

/** Resolves a path the way the gateway does: the first route, in order, whose predicate matches. */
final class Routes {

    private Routes() {
    }

    static String firstMatch(RouteLocator routeLocator, String path) {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get(path));
        return routeLocator.getRoutes()
                .concatMap(route -> Mono.from(route.getPredicate().apply(exchange))
                        .filter(Boolean::booleanValue)
                        .map(matched -> route.getId()))
                .blockFirst(Duration.ofSeconds(10));
    }
}
