package com.reactivespring.gateway.filter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * By the time the gateway filter chain completes, the proxied response has been written and its
 * headers are read-only, so response headers must be added before commit.
 */
@DisplayName("Filters that add response headers")
class ResponseHeaderFiltersTest {

    /** Completes the response the way NettyWriteResponseFilter does at the end of the chain. */
    private static final GatewayFilterChain WRITES_RESPONSE = exchange -> exchange.getResponse().setComplete();

    @Test
    @DisplayName("GlobalLoggingFilter puts X-Correlation-Id on the response")
    void correlationIdReachesTheResponse() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/v1/movies/1")
                .header(GlobalLoggingFilter.CORRELATION_ID_HEADER, "cid-1"));

        StepVerifier.create(new GlobalLoggingFilter().filter(exchange, WRITES_RESPONSE)).verifyComplete();

        assertThat(exchange.getResponse().getHeaders().getFirst(GlobalLoggingFilter.CORRELATION_ID_HEADER))
                .isEqualTo("cid-1");
    }

    @Test
    @DisplayName("PostFilter puts X-Response-Time-Ms on the response")
    void responseTimeReachesTheResponse() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/v1/movies/1")
                .header("X-Request-Start", String.valueOf(System.currentTimeMillis())));
        GatewayFilter postFilter = new PostFilterGatewayFilterFactory().apply(new PostFilterGatewayFilterFactory.Config());

        StepVerifier.create(postFilter.filter(exchange, WRITES_RESPONSE)).verifyComplete();

        assertThat(exchange.getResponse().getHeaders().getFirst("X-Response-Time-Ms")).isNotNull();
    }
}
