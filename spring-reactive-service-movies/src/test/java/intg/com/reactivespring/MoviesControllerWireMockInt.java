package com.reactivespring;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.reactivespring.entity.Movie;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@TestPropertySource(properties = {
        "movies.url.movieInfo=http://localhost:8084/v1/movieInfo",
        "movies.url.reviews=http://localhost:8084/v1/reviews"
})
class MoviesControllerWireMockInt {

    static WireMockServer wireMockServer;

    @LocalServerPort
    int port;

    WebTestClient webTestClient;

    private static final String MOVIE_INFO_JSON = """
            {"movieInfoId":"1","name":"Batman Begins","year":2005,"cast":["Christian Bale","Michael Cane"],"release_date":"2005-06-15"}
            """;

    private static final String REVIEWS_JSON = """
            [{"reviewId":"1","movieInfoId":1,"comment":"Awesome Movie","rating":9.0},{"reviewId":"2","movieInfoId":1,"comment":"Excellent Movie","rating":8.0}]
            """;

    @BeforeAll
    static void startWireMock() {
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().port(8084));
        wireMockServer.start();
        WireMock.configureFor("localhost", 8084);
    }

    @AfterAll
    static void stopWireMock() {
        wireMockServer.stop();
    }

    @BeforeEach
    void setup() {
        webTestClient = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
        WireMock.reset();
    }

    @Test
    @DisplayName("GET /v1/movies/{id} — aggregates movie info and reviews")
    void retrieveMovieById_returnsAggregatedMovie() {
        var movieId = "abc";

        stubFor(get(urlEqualTo("/v1/movieInfo/" + movieId))
                .willReturn(aResponse()
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                        .withBody(MOVIE_INFO_JSON)));

        stubFor(get(urlPathEqualTo("/v1/reviews"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                        .withBody(REVIEWS_JSON)));

        webTestClient.get()
                .uri("/v1/movies/{id}", movieId)
                .exchange()
                .expectStatus().isOk()
                .expectBody(Movie.class)
                .consumeWith(result -> {
                    var movie = result.getResponseBody();
                    assertThat(movie).isNotNull();
                    assertThat(movie.movieInfo().movieInfoId()).isEqualTo("1");
                    assertThat(movie.movieInfo().name()).isEqualTo("Batman Begins");
                    assertThat(movie.reviewList()).hasSize(2);
                    assertThat(movie.reviewList().get(0).comment()).isEqualTo("Awesome Movie");
                    assertThat(movie.reviewList().get(1).comment()).isEqualTo("Excellent Movie");
                });
    }

    @Test
    @DisplayName("GET /v1/movies/{id} — returns 404 when movie info not found")
    void retrieveMovieById_returns404WhenMovieInfoMissing() {
        var movieId = "unknown";

        stubFor(get(urlEqualTo("/v1/movieInfo/" + movieId))
                .willReturn(aResponse()
                        .withStatus(404)
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                        .withBody("Movie info not found")));

        webTestClient.get()
                .uri("/v1/movies/{id}", movieId)
                .exchange()
                .expectStatus().isNotFound();
    }
}
