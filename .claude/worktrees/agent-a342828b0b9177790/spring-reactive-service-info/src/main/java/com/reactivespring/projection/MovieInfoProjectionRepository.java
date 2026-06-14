package com.reactivespring.projection;

import com.reactivespring.entity.MovieInfoDocument;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface MovieInfoProjectionRepository extends ReactiveMongoRepository<MovieInfoDocument, String> {

    // Fields projection: Spring Data converts the return type to MovieSummary automatically
    // when @Query includes a fields parameter specifying which fields to include.
    @Query(value = "{}", fields = "{ 'movieInfoId': 1, 'name': 1, 'year': 1 }")
    Flux<MovieSummary> findAllSummaries();

    @Query(value = "{ '_id': ?0 }", fields = "{ 'movieInfoId': 1, 'name': 1, 'year': 1 }")
    Mono<MovieSummary> findSummaryById(String id);
}
