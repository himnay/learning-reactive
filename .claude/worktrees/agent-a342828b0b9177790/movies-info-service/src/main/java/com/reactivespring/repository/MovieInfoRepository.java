package com.reactivespring.repository;

import com.reactivespring.entity.MovieInfoDocument;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface MovieInfoRepository extends ReactiveMongoRepository<MovieInfoDocument, String> {

    Flux<MovieInfoDocument> findByYear(Integer year);

    Flux<MovieInfoDocument> findByName(String name);

}
