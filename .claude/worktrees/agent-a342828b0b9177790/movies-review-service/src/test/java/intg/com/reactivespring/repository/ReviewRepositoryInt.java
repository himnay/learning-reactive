package com.reactivespring.repository;

import com.reactivespring.entity.ReviewDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.test.context.ActiveProfiles;
import reactor.test.StepVerifier;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@DataMongoTest
@ActiveProfiles("test")
public class ReviewRepositoryInt {

    @Autowired
    private ReviewRepository reviewRepository;

    @BeforeEach
    public void init() {
        var reviewInfos = List.of(
                new ReviewDocument(null, 1l, "awesome movie", 9.0),
                new ReviewDocument(null, 2l, "awesome movie", 9.0),
                new ReviewDocument("abc", 3l, "awesome movie", 8.0)
        );

        // blockLast() as all apis are async and nonblocking. this will make sure all data are persisted before any test get execute
        // should be only used for testing only
        reviewRepository.saveAll(reviewInfos).blockLast();
    }

    @Test
    @DisplayName("Test all review info")
    public void findAllMovieInfoTest() {
        var movieInfos = reviewRepository.findAll().log();

        StepVerifier.create(movieInfos)
                .expectNextCount(3)
                .expectComplete();
    }

    @Test
    @DisplayName("Test a single review info")
    public void findAMovieInfoTest() {
        var movieInfos = reviewRepository.findById("abc").log();

        StepVerifier.create(movieInfos)
                .assertNext(i -> {
                    assertThat(i, is("Dark Knight Rises"));
                })
                .expectComplete();
    }

    @Test
    @DisplayName("delete a single review info")
    public void deleteMovieInfoTest() {
        reviewRepository.deleteById("abc").block();

        var movieInfos = reviewRepository.findAll();
        StepVerifier.create(movieInfos)
                .expectNextCount(3)
                .expectComplete();
    }

    @AfterEach
    public void tearDown() {
        reviewRepository.deleteAll();
    }
}
