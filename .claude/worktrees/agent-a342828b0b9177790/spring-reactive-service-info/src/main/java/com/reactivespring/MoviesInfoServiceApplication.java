package com.reactivespring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

@SpringBootApplication
@EnableScheduling
public class MoviesInfoServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(MoviesInfoServiceApplication.class, args);
    }

    /**
     * WebSocketHandlerAdapter is required when using SimpleUrlHandlerMapping for WebSocket
     * routing without Spring Security's WebSocket support.
     */
    @Bean
    public WebSocketHandlerAdapter webSocketHandlerAdapter() {
        return new WebSocketHandlerAdapter();
    }

    /**
     * Tailable cursors require a MongoDB capped collection.  We create it on startup
     * if it doesn't already exist (idempotent — createCollection is a no-op if the
     * collection exists with compatible options).  Max size 1MB, max 1000 documents.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void ensureCappedCollection(ApplicationReadyEvent event) {
        ReactiveMongoTemplate template = event.getApplicationContext()
                .getBean(ReactiveMongoTemplate.class);

        template.collectionExists("movieInfoDocument")
                .flatMap(exists -> {
                    if (!exists) {
                        return template.createCollection("movieInfoDocument",
                                CollectionOptions.empty()
                                        .capped()
                                        .maxDocuments(1000)
                                        .size(1024 * 1024));
                    }
                    return reactor.core.publisher.Mono.empty();
                })
                .subscribe();
    }
}
