package com.reactivespring.repository;

import com.reactivespring.entity.MovieInfoDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.test.context.ActiveProfiles;
import reactor.test.StepVerifier;

import java.time.LocalDate;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@DataMongoTest
@ActiveProfiles("test")
public class MovieInfoRepositoryInt {

    @Autowired
    private MovieInfoRepository movieInfoRepository;

    @BeforeEach
    public void init() {
        var movieInfos = List.of(
                new MovieInfoDocument(null, "Batman Begins", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15")),
                new MovieInfoDocument(null, "The Dark Knight", 2012, List.of("Christian Bale", "HeathLedger"), LocalDate.parse("2008-07-18")),
                new MovieInfoDocument("abc", "The Dark Knight", 2012, List.of("Christian Bale", "Tom Hardy"), LocalDate.parse("2012-07-20"))
        );

        // blockLast() as all apis are async and nonblocking. this will make sure all data are persisted before any test get execute
        // should be only used for testing only
        movieInfoRepository.saveAll(movieInfos).blockLast();
    }

    @Test
    @DisplayName("Test all movie info")
    public void findAllMovieInfoTest() {
        var movieInfos = movieInfoRepository.findAll().log();

        StepVerifier.create(movieInfos)
                .expectNextCount(3)
                .expectComplete();
    }

    @Test
    @DisplayName("Test a single movie info")
    public void findAMovieInfoTest() {
        var movieInfos = movieInfoRepository.findById("abc").log();

        StepVerifier.create(movieInfos)
                .assertNext(i -> {
                    assertThat(i, is("Dark Knight Rises"));
                })
                .expectComplete();
    }

    @Test
    @DisplayName("Test a single movie info by release year")
    public void findAMovieInfoByYearTest() {
        var movieInfos = movieInfoRepository.findByYear(2012).log();

        StepVerifier.create(movieInfos)
                .expectNextCount(2)
                .expectComplete();
    }

    @Test
    @DisplayName("Test a single movie info by name")
    public void findAMovieInfoByNameTest() {
        var movieInfos = movieInfoRepository.findByName("Batman Begins").log();

        StepVerifier.create(movieInfos)
                .expectNextCount(1)
                .expectComplete();
    }

    @Test
    @DisplayName("save a single movie info")
    public void saveMovieInfoTest() {
        MovieInfoDocument movieInfoDocument = new MovieInfoDocument(null, "Batman Begins 2", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15"));
        var movieInfos = movieInfoRepository.save(movieInfoDocument).log();

        StepVerifier.create(movieInfos)
                .assertNext(i -> {
                    assertThat(i.getName(), is("Batman Begins 2"));
                })
                .expectComplete();
    }

    @Test
    @DisplayName("update a single movie info, block() gets entity object from Mono publisher")
    public void updateMovieInfoTest() {
        var movieInfo = movieInfoRepository.findById("abc").block();
        movieInfo.setName("Batman Begins 3");

        var movieInfoMono = movieInfoRepository.save(movieInfo);

        StepVerifier.create(movieInfoMono)
                .assertNext(i -> {
                    assertThat(i.getName(), is("Batman Begins 3"));
                })
                .expectComplete();
    }

    @Test
    @DisplayName("delete a single movie info")
    public void deleteMovieInfoTest() {
        movieInfoRepository.deleteById("abc").block();

        var movieInfos = movieInfoRepository.findAll();
        StepVerifier.create(movieInfos)
                .expectNextCount(3)
                .expectComplete();
    }

    @AfterEach
    public void tearDown() {
        movieInfoRepository.deleteAll();
    }
}
