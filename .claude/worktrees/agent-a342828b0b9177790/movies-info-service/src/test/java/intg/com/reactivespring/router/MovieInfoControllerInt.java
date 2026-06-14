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
import org.springframework.web.util.UriComponentsBuilder;

import java.time.LocalDate;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@SpringBootTest
@ActiveProfiles("test")
@AutoConfigureWebTestClient
public class MovieInfoControllerInt {

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
    @DisplayName("test POST movies")
    public void createMovieInfoTest() {
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
    }

    @Test
    @DisplayName("test GET list of movies")
    public void getAllMoviesTest() {
        webTestClient.get()
                .uri("/v1/movieInfos")
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .expectBodyList(MovieInfoDocument.class)
                .consumeWith(i -> {
                    var movieInfos = i.getResponseBody();
                    assert movieInfos.size() >= 3;
                });
    }

    @Test
    @DisplayName("test GET list of movies by year")
    public void getAllMoviesByYearTest() {
        var uri = UriComponentsBuilder.fromUriString("/v1/movieInfos")
                .queryParam("year", 2012)
                .buildAndExpand().toUri();

        webTestClient.get()
                .uri(uri)
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .expectBodyList(MovieInfoDocument.class)
                .consumeWith(i -> {
                    var movieInfos = i.getResponseBody();
                    assert movieInfos.size() == 2;
                });
    }

    @Test
    @DisplayName("test GET list of movies by name")
    public void getAllMoviesByNameTest() {
        var uri = UriComponentsBuilder.fromUriString("/v1/movieInfos")
                .queryParam("name", "Batman Begins")
                .buildAndExpand().toUri();

        webTestClient.get()
                .uri(uri)
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .expectBodyList(MovieInfoDocument.class)
                .consumeWith(i -> {
                    var movieInfos = i.getResponseBody();
                    assert movieInfos.size() == 1;
                });
    }

    @Test
    @DisplayName("GET() by id")
    public void getByIdMovieInfoTest() {
        var movieInfoId = "abc";

        var responseBody = webTestClient.get()
                .uri("/v1/movieInfo/{id}", movieInfoId)
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .expectBody(MovieInfoDocument.class)
                .consumeWith(i -> {
                    var movieInfoResponse = i.getResponseBody();
                    assertThat(movieInfoResponse.getMovieInfoId(), is("abc"));
                    assertThat(movieInfoResponse.getName(), is("Dark Knight Rises"));
                });
    }

    @Test
    @DisplayName("GET by id")
    public void getByIdMovieInfoJSONPathTest() {
        var movieInfoId = "abc";

        var responseBody = webTestClient.get()
                .uri("/v1/movieInfo/{id}", movieInfoId)
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .expectBody()
                .jsonPath("$.movieInfoId").isEqualTo("abc")
                .jsonPath("$.name").isEqualTo("Dark Knight Rises");
    }

    @Test
    @DisplayName("GET by id not found")
    public void getByIdMovieInfoNotFoundTest() {
        webTestClient.get()
                .uri("/v1/movieInfo/{id}", "xyz")
                .exchange()
                .expectStatus()
                .isNotFound()
                .expectHeader().valueEquals("X-Reason", "movie info id cannot be found");
    }

    @Test
    @DisplayName("PUT movieInfo")
    public void upsertMovieInfoTest() {
        var movieInfo = new MovieInfoDocument("", "Batman Begins 2", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15"));

        webTestClient
                .put()
                .uri("/v1/movieInfo/{id}", "abc")
                .bodyValue(movieInfo)
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .expectBody(MovieInfoDocument.class)
                .consumeWith(i -> {
                    var movieInfoResponse = i.getResponseBody();
                    assert movieInfoResponse.getMovieInfoId() != null;
                    assertThat(movieInfoResponse.getName(), is("Batman Begins 2"));
                });
    }

    @Test
    @DisplayName("PUT movieInfo when not found")
    public void upsertMovieInfoNotFoundTest() {
        var movieInfo = new MovieInfoDocument("xyz", "Batman Begins 2", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15"));

        webTestClient
                .put()
                .uri("/v1/movieInfo/{id}", "xyz")
                .bodyValue(movieInfo)
                .exchange()
                .expectStatus()
                .isNotFound();
    }

    @Test
    @DisplayName("DELETE movieInfo")
    public void deleteMovieInfoTest() {
        webTestClient
                .delete()
                .uri("/v1/movieInfo/{id}", "abc")
                .exchange()
                .expectStatus()
                .isNoContent()
                .expectBody(Void.class);
    }
}
