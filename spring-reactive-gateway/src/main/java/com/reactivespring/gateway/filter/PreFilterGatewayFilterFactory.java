package com.reactivespring.gateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;

/**
 * Named per-route filter factory: {@code - name: PreFilter} in YAML routes.
 * Runs BEFORE the request is forwarded downstream:
 * - validates / enriches request headers
 * - stamps a gateway version header
 *
 * Factory name = class name minus "GatewayFilterFactory" suffix → "PreFilter"
 */
@Component
public class PreFilterGatewayFilterFactory
        extends AbstractGatewayFilterFactory<PreFilterGatewayFilterFactory.Config> {

    private static final Logger log = LoggerFactory.getLogger(PreFilterGatewayFilterFactory.class);

    public PreFilterGatewayFilterFactory() {
        super(Config.class);
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            ServerHttpRequest req = exchange.getRequest();
            log.debug("[PreFilter] → {} {} (requestId={})", req.getMethod(), req.getPath(),
                    req.getId());

            ServerHttpRequest enriched = req.mutate()
                    .header("X-Gateway-Version", "1.0")
                    .header("X-Request-Start", String.valueOf(System.currentTimeMillis()))
                    .build();

            return chain.filter(exchange.mutate().request(enriched).build());
        };
    }

    public static class Config {
        // add configurable fields here if needed, e.g. a custom header name
    }
}
