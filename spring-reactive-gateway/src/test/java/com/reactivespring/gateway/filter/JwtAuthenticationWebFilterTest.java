package com.reactivespring.gateway.filter;

import io.jsonwebtoken.Jwts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.OrderUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;

import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("JwtAuthenticationWebFilter")
class JwtAuthenticationWebFilterTest {

    private static final String SECRET = "test-only-secret-key-with-at-least-256-bits!!";

    private final JwtAuthenticationWebFilter filter = new JwtAuthenticationWebFilter();

    @BeforeEach
    void setSecret() {
        ReflectionTestUtils.setField(filter, "jwtSecret", SECRET);
    }

    /** Runs the filter; returns the request the rest of the chain received, or null if it stopped. */
    private ServerHttpRequest forwarded(MockServerWebExchange exchange) {
        AtomicReference<ServerHttpRequest> seen = new AtomicReference<>();
        filter.filter(exchange, next -> {
            seen.set(next.getRequest());
            return Mono.empty();
        }).block();
        return seen.get();
    }

    @Test
    @DisplayName("a protected path without a Bearer token gets 401")
    void rejectsMissingToken() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/v1/movies/1"));

        assertThat(forwarded(exchange)).isNull();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("a valid token sets X-User-Id from its subject, replacing a client-supplied value")
    void validTokenSetsUserIdFromSubject() {
        String token = TokenUtil.generateToken("alice", SECRET, 60_000);
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/v1/movies/1")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header("X-User-Id", "mallory"));

        assertThat(forwarded(exchange).getHeaders().get("X-User-Id")).containsExactly("alice");
    }

    @Test
    @DisplayName("a public path forwards no client-supplied X-User-Id (downstream and the rate limiter trust it)")
    void publicPathDropsClientSuppliedUserId() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/v1/public/info")
                .header("X-User-Id", "mallory"));

        ServerHttpRequest request = forwarded(exchange);
        assertThat(request).isNotNull();
        assertThat(request.getHeaders().getFirst("X-User-Id")).isNull();
    }

    @Test
    @DisplayName("a signed token without a subject gets 401")
    void rejectsTokenWithoutSubject() {
        String token = Jwts.builder()
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + 60_000))
                .signWith(TokenUtil.signingKey(SECRET))
                .compact();
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/v1/movies/1")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token));

        assertThat(forwarded(exchange)).isNull();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("RequestIdWebFilter runs before JwtAuthenticationWebFilter")
    void requestIdFilterRunsFirst() {
        assertThat(OrderUtils.getOrder(RequestIdWebFilter.class, 0))
                .isLessThan(OrderUtils.getOrder(JwtAuthenticationWebFilter.class, 0));
    }
}
