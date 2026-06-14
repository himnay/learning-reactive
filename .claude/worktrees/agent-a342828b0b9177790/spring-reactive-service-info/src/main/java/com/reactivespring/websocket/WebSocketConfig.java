package com.reactivespring.websocket;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;

import java.util.Map;

@Configuration
public class WebSocketConfig {

    @Bean
    public HandlerMapping webSocketHandlerMapping(MovieInfoWebSocketHandler handler) {
        // SimpleUrlHandlerMapping wires URL paths to WebSocketHandler beans.
        // Order must be lower (higher precedence) than the default DispatcherHandler
        // mapping so that WebSocket upgrade requests are routed before MVC catches them.
        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setUrlMap(Map.of("/ws/movieInfo", handler));
        mapping.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return mapping;
    }
}
