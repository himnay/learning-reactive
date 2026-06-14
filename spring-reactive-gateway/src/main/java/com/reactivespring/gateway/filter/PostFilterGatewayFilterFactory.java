package com.reactivespring.gateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.stereotype.Component;

/**
 * Named per-route filter factory: {@code - name: PostFilter} in YAML routes.
 * Runs AFTER the downstream response returns:
 * - adds response-time header
 * - logs status code and latency
 *
 * Factory name = class name minus "GatewayFilterFactory" suffix → "PostFilter"
 */
@Component
public class PostFilterGatewayFilterFactory
        extends AbstractGatewayFilterFactory<PostFilterGatewayFilterFactory.Config> {

    private static final Logger log = LoggerFactory.getLogger(PostFilterGatewayFilterFactory.class);

    public PostFilterGatewayFilterFactory() {
        super(Config.class);
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> chain.filter(exchange).then(
                reactor.core.publisher.Mono.fromRunnable(() -> {
                    String startHeader = exchange.getRequest().getHeaders().getFirst("X-Request-Start");
                    if (startHeader != null) {
                        long elapsed = System.currentTimeMillis() - Long.parseLong(startHeader);
                        exchange.getResponse().getHeaders().add("X-Response-Time-Ms", String.valueOf(elapsed));
                        log.debug("[PostFilter] ← {} {} {}ms",
                                exchange.getResponse().getStatusCode(),
                                exchange.getRequest().getPath(), elapsed);
                    }
                })
        );
    }

    public static class Config {
    }
}
