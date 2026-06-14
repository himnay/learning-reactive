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
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Testcontainers
class MovieInfoControllerSSEInt {

    @Container
    @ServiceConnection
    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7");

    @Autowired
    private MovieInfoRepository movieInfoRepository;

    @Autowired
    private WebTestClient webTestClient;

    @BeforeEach
    void init() {
        movieInfoRepository.saveAll(List.of(
                new MovieInfoDocument(null, "Batman Begins", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15")),
                new MovieInfoDocument(null, "The Dark Knight", 2012, List.of("Christian Bale", "HeathLedger"), LocalDate.parse("2008-07-18")),
                new MovieInfoDocument("abc", "Dark Knight Rises", 2012, List.of("Christian Bale", "Tom Hardy"), LocalDate.parse("2012-07-20"))
        )).blockLast();
    }

    @AfterEach
    void tearDown() {
        movieInfoRepository.deleteAll().block();
    }

    @Test
    @DisplayName("POST then GET /stream — newly created movie appears on SSE stream")
    void createMovieInfoAndStreamUsingSSETest() {
        var movieInfo = new MovieInfoDocument(null, "Batman Begins 2", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15"));

        webTestClient.post()
                .uri("/v1/movieInfo")
                .bodyValue(movieInfo)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(MovieInfoDocument.class)
                .consumeWith(resp -> assertThat(resp.getResponseBody().movieInfoId()).isNotNull());

        var stream = webTestClient.get()
                .uri("/v1/movieInfo/stream")
                .exchange()
                .expectStatus().is2xxSuccessful()
                .returnResult(MovieInfoDocument.class)
                .getResponseBody();

        StepVerifier.create(stream)
                .assertNext(doc -> assertThat(doc.movieInfoId()).isNotNull())
                .thenCancel()
                .verify();
    }
}
