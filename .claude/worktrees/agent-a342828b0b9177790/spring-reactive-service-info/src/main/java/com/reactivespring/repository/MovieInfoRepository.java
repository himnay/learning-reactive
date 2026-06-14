package com.reactivespring.repository;

import com.reactivespring.entity.MovieInfoDocument;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.Tailable;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface MovieInfoRepository extends ReactiveMongoRepository<MovieInfoDocument, String> {

    Flux<MovieInfoDocument> findByYear(Integer year);

    Flux<MovieInfoDocument> findByName(String name);

    // Tailable cursor streams new documents as they are inserted into a capped collection.
    // Requires the collection to be capped (fixed max size); see MoviesInfoServiceApplication
    // for the ApplicationReadyEvent listener that ensures the capped collection exists.
    @Tailable
    Flux<MovieInfoDocument> findWithTailableCursorBy();
}
