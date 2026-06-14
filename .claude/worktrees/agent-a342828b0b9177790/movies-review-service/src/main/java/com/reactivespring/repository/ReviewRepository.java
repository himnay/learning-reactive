package com.reactivespring.repository;

import com.reactivespring.entity.ReviewDocument;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface ReviewRepository extends ReactiveMongoRepository<ReviewDocument, String> {

    Flux<ReviewDocument> findByMovieInfoId(Long movieInfoId);
}
