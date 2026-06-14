package com.reactivespring.repository;

import com.reactivespring.entity.MovieInfoDocument;
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

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DataMongoTest
@Testcontainers
class MovieInfoRepositoryInt {

    @Container
    @ServiceConnection
    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7");

    @Autowired
    private MovieInfoRepository movieInfoRepository;

    @BeforeEach
    void init() {
        var movieInfos = List.of(
                new MovieInfoDocument(null, "Batman Begins", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15")),
                new MovieInfoDocument(null, "The Dark Knight", 2012, List.of("Christian Bale", "HeathLedger"), LocalDate.parse("2008-07-18")),
                new MovieInfoDocument("abc", "The Dark Knight", 2012, List.of("Christian Bale", "Tom Hardy"), LocalDate.parse("2012-07-20"))
        );
        movieInfoRepository.saveAll(movieInfos).blockLast();
    }

    @AfterEach
    void tearDown() {
        movieInfoRepository.deleteAll().block();
    }

    @Test
    @DisplayName("findAll returns all seeded documents")
    void findAllMovieInfoTest() {
        StepVerifier.create(movieInfoRepository.findAll().log())
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    @DisplayName("findById returns the correct document")
    void findAMovieInfoTest() {
        StepVerifier.create(movieInfoRepository.findById("abc").log())
                .assertNext(doc -> assertThat(doc.name()).isEqualTo("The Dark Knight"))
                .verifyComplete();
    }

    @Test
    @DisplayName("findByYear returns matching documents")
    void findAMovieInfoByYearTest() {
        StepVerifier.create(movieInfoRepository.findByYear(2012).log())
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    @DisplayName("findByName returns matching documents")
    void findAMovieInfoByNameTest() {
        StepVerifier.create(movieInfoRepository.findByName("Batman Begins").log())
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    @DisplayName("save creates a new document with generated ID")
    void saveMovieInfoTest() {
        var doc = new MovieInfoDocument(null, "Batman Begins 2", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15"));
        StepVerifier.create(movieInfoRepository.save(doc).log())
                .assertNext(saved -> {
                    assertThat(saved.movieInfoId()).isNotNull();
                    assertThat(saved.name()).isEqualTo("Batman Begins 2");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("update by creating a new record instance preserves ID")
    void updateMovieInfoTest() {
        var found = movieInfoRepository.findById("abc").block();
        assertThat(found).isNotNull();

        // Records are immutable — create a new instance with the updated field
        var updated = new MovieInfoDocument(found.movieInfoId(), "Batman Begins 3", found.year(), found.cast(), found.releaseDate());

        StepVerifier.create(movieInfoRepository.save(updated).log())
                .assertNext(saved -> assertThat(saved.name()).isEqualTo("Batman Begins 3"))
                .verifyComplete();
    }

    @Test
    @DisplayName("delete removes the document")
    void deleteMovieInfoTest() {
        movieInfoRepository.deleteById("abc").block();
        StepVerifier.create(movieInfoRepository.findAll())
                .expectNextCount(2)
                .verifyComplete();
    }
}
