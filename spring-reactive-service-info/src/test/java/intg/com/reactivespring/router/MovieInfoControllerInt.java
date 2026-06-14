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
import org.springframework.web.util.UriComponentsBuilder;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Testcontainers
class MovieInfoControllerInt {

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
    @DisplayName("POST /v1/movieInfo — creates a new movie")
    void createMovieInfoTest() {
        var movieInfo = new MovieInfoDocument(null, "Batman Begins 2", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15"));
        webTestClient.post()
                .uri("/v1/movieInfo")
                .bodyValue(movieInfo)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(MovieInfoDocument.class)
                .consumeWith(resp -> assertThat(resp.getResponseBody().movieInfoId()).isNotNull());
    }

    @Test
    @DisplayName("GET /v1/movieInfos — returns all movies")
    void getAllMoviesTest() {
        webTestClient.get()
                .uri("/v1/movieInfos")
                .exchange()
                .expectStatus().is2xxSuccessful()
                .expectBodyList(MovieInfoDocument.class)
                .consumeWith(resp -> assertThat(resp.getResponseBody()).hasSizeGreaterThanOrEqualTo(3));
    }

    @Test
    @DisplayName("GET /v1/movieInfos?year=2012 — filters by year")
    void getAllMoviesByYearTest() {
        var uri = UriComponentsBuilder.fromUriString("/v1/movieInfos")
                .queryParam("year", 2012).buildAndExpand().toUri();
        webTestClient.get().uri(uri).exchange()
                .expectStatus().is2xxSuccessful()
                .expectBodyList(MovieInfoDocument.class)
                .consumeWith(resp -> assertThat(resp.getResponseBody()).hasSize(2));
    }

    @Test
    @DisplayName("GET /v1/movieInfos?name=Batman Begins — filters by name")
    void getAllMoviesByNameTest() {
        var uri = UriComponentsBuilder.fromUriString("/v1/movieInfos")
                .queryParam("name", "Batman Begins").buildAndExpand().toUri();
        webTestClient.get().uri(uri).exchange()
                .expectStatus().is2xxSuccessful()
                .expectBodyList(MovieInfoDocument.class)
                .consumeWith(resp -> assertThat(resp.getResponseBody()).hasSize(1));
    }

    @Test
    @DisplayName("GET /v1/movieInfo/{id} — returns the correct movie")
    void getByIdMovieInfoTest() {
        webTestClient.get().uri("/v1/movieInfo/{id}", "abc")
                .exchange()
                .expectStatus().is2xxSuccessful()
                .expectBody(MovieInfoDocument.class)
                .consumeWith(resp -> {
                    var doc = resp.getResponseBody();
                    assertThat(doc.movieInfoId()).isEqualTo("abc");
                    assertThat(doc.name()).isEqualTo("Dark Knight Rises");
                });
    }

    @Test
    @DisplayName("GET /v1/movieInfo/{id} — JSON path assertions")
    void getByIdMovieInfoJSONPathTest() {
        webTestClient.get().uri("/v1/movieInfo/{id}", "abc")
                .exchange()
                .expectStatus().is2xxSuccessful()
                .expectBody()
                .jsonPath("$.movieInfoId").isEqualTo("abc")
                .jsonPath("$.name").isEqualTo("Dark Knight Rises");
    }

    @Test
    @DisplayName("GET /v1/movieInfo/{id} — 404 for unknown id")
    void getByIdMovieInfoNotFoundTest() {
        webTestClient.get().uri("/v1/movieInfo/{id}", "xyz")
                .exchange()
                .expectStatus().isNotFound()
                .expectHeader().valueEquals("X-Reason", "movie info id cannot be found");
    }

    @Test
    @DisplayName("PUT /v1/movieInfo/{id} — updates existing movie")
    void upsertMovieInfoTest() {
        var updated = new MovieInfoDocument("", "Batman Begins 2", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15"));
        webTestClient.put().uri("/v1/movieInfo/{id}", "abc")
                .bodyValue(updated)
                .exchange()
                .expectStatus().is2xxSuccessful()
                .expectBody(MovieInfoDocument.class)
                .consumeWith(resp -> {
                    var doc = resp.getResponseBody();
                    assertThat(doc.movieInfoId()).isNotNull();
                    assertThat(doc.name()).isEqualTo("Batman Begins 2");
                });
    }

    @Test
    @DisplayName("PUT /v1/movieInfo/{id} — 404 for unknown id")
    void upsertMovieInfoNotFoundTest() {
        var updated = new MovieInfoDocument("xyz", "Batman Begins 2", 2005, List.of("Christian Bale"), LocalDate.parse("2005-06-15"));
        webTestClient.put().uri("/v1/movieInfo/{id}", "xyz")
                .bodyValue(updated)
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    @DisplayName("DELETE /v1/movieInfo/{id} — removes movie")
    void deleteMovieInfoTest() {
        webTestClient.delete().uri("/v1/movieInfo/{id}", "abc")
                .exchange()
                .expectStatus().isNoContent()
                .expectBody(Void.class);
    }
}
