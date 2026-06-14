package com.reactivespring.repository;

import com.reactivespring.entity.ReviewDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DataMongoTest
@Testcontainers
public class ReviewRepositoryInt {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:7");

    @Autowired
    private ReviewRepository reviewRepository;

    @BeforeEach
    public void init() {
        var reviewInfos = List.of(
                new ReviewDocument(null, 1L, "awesome movie", 9.0),
                new ReviewDocument(null, 2L, "awesome movie", 9.0),
                new ReviewDocument("abc", 3L, "awesome movie", 8.0)
        );
        reviewRepository.saveAll(reviewInfos).blockLast();
    }

    @AfterEach
    public void tearDown() {
        reviewRepository.deleteAll().block();
    }

    @Test
    @DisplayName("find all reviews")
    public void findAllMovieInfoTest() {
        StepVerifier.create(reviewRepository.findAll().log())
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    @DisplayName("find a single review by id")
    public void findAMovieInfoTest() {
        StepVerifier.create(reviewRepository.findById("abc").log())
                .assertNext(doc -> assertThat(doc.reviewId()).isEqualTo("abc"))
                .verifyComplete();
    }

    @Test
    @DisplayName("delete a single review")
    public void deleteMovieInfoTest() {
        reviewRepository.deleteById("abc").block();

        StepVerifier.create(reviewRepository.findAll())
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    @DisplayName("find reviews by movieInfoId")
    public void findByMovieInfoIdTest() {
        StepVerifier.create(reviewRepository.findByMovieInfoId(3L).log())
                .assertNext(doc -> {
                    assertThat(doc.reviewId()).isEqualTo("abc");
                    assertThat(doc.rating()).isEqualTo(8.0);
                })
                .verifyComplete();
    }
}
