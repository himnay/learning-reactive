package com.reactivespring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MoviesInfoServiceApplication {

    private static final Logger log = LoggerFactory.getLogger(MoviesInfoServiceApplication.class);

    private final ReactiveMongoTemplate mongoTemplate;

    public MoviesInfoServiceApplication(ReactiveMongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public static void main(String[] args) {
        SpringApplication.run(MoviesInfoServiceApplication.class, args);
    }

    // Tailable cursors require a capped collection. Create it on startup if absent.
    // Max size 1MB — oldest documents are evicted when the cap is reached.
    @EventListener(ApplicationReadyEvent.class)
    public void createCappedCollection() {
        mongoTemplate.collectionExists("movieInfoDocument")
                .filter(exists -> !exists)
                .flatMap(notExists -> mongoTemplate.createCollection(
                        "movieInfoDocument",
                        CollectionOptions.empty().capped().size(1_048_576).maxDocuments(500)))
                .subscribe(
                        c -> log.info("Created capped collection: {}", c.getNamespace()),
                        e -> log.debug("Collection already exists or creation skipped: {}", e.getMessage())
                );
    }
}
