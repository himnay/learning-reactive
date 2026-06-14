package com.reactivespring.router;

import com.reactivespring.entity.MovieInfoDocument;
import com.reactivespring.repository.MovieInfoRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.test.StepVerifier;

import java.time.LocalDate;
import java.util.List;

@SpringBootTest
@ActiveProfiles("test")
@AutoConfigureWebTestClient
public class MovieInfoControllerSSEInt {

    @Autowired
    private MovieInfoRepository movieInfoRepository;

    @Autowired
    private WebTestClient webTestClient;

    @BeforeEach
    public void init() {
        var movieInfos = List.of(
                new MovieInfoDocument(null, "Batman Begins", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15")),
                new MovieInfoDocument(null, "The Dark Knight", 2012, List.of("Christian Bale", "HeathLedger"), LocalDate.parse("2008-07-18")),
                new MovieInfoDocument("abc", "Dark Knight Rises", 2012, List.of("Christian Bale", "Tom Hardy"), LocalDate.parse("2012-07-20"))
        );

        // blockLast() as all apis are async and nonblocking. this will make sure all data are persisted before any test get execute
        // should be only used for testing only
        movieInfoRepository.saveAll(movieInfos).blockLast();
    }

    @AfterEach
    public void tearDown() {
        movieInfoRepository.deleteAll().block();
    }

    @Test
    @DisplayName("test SSE streaming movies")
    public void createMovieInfoAndStreamUsingSSETest() {
        var movieInfo = new MovieInfoDocument(null, "Batman Begins 2", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15"));

        webTestClient
                .post()
                .uri("/v1/movieInfo")
                .bodyValue(movieInfo)
                .exchange()
                .expectStatus()
                .isCreated()
                .expectBody(MovieInfoDocument.class)
                .consumeWith(i -> {
                    var movieInfoResponse = i.getResponseBody();
                    assert movieInfoResponse.getMovieInfoId() != null;
                });

        var response = webTestClient.get()
                .uri("/v1/movieInfo/stream")
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .returnResult(MovieInfoDocument.class)
                .getResponseBody();

        StepVerifier
                .create(response)
                .assertNext(i -> {
                    assert i.getMovieInfoId() != null;
                })
                .thenCancel()
                .verify();
    }
}
