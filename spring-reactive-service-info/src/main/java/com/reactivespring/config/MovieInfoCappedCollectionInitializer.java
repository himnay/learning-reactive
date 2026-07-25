package com.reactivespring.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.stereotype.Component;

/**
 * Creates the capped {@code movieInfoDocument} collection (required for tailable cursors) on
 * startup if it doesn't already exist. Kept out of the {@code @SpringBootApplication} class so
 * that {@code ReactiveMongoTemplate} isn't a mandatory dependency of the app's configuration
 * bean itself — that would force every web-layer slice test (e.g. {@code @WebFluxTest}) to load
 * a Mongo bean it has no other reason to need.
 */
@Component
class MovieInfoCappedCollectionInitializer {

    private static final Logger log = LoggerFactory.getLogger(MovieInfoCappedCollectionInitializer.class);

    private final ReactiveMongoTemplate mongoTemplate;

    public MovieInfoCappedCollectionInitializer(ReactiveMongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
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
