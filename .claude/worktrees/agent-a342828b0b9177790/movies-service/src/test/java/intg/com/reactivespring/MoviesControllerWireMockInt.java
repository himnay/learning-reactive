package com.reactivespring;

import com.reactivespring.entity.Movie;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@SpringBootTest
@ActiveProfiles("test")
@AutoConfigureWebTestClient
@AutoConfigureWireMock(port = 8084)
@TestPropertySource(
        properties = {
                "movies.url.movieInfo=http://localhost:8084/v1/movieInfo",
                "movies.url.reviews=http://localhost:8084/v1/reviews"
        }
)
public class MoviesControllerWireMockInt {

    @Autowired
    private WebTestClient webTestClient;

    @Test
    @DisplayName("GET movie by id")
    public void retrieveMovieByIdUsingWiremock() {
        var movieId = "abc";

        stubFor(get(urlEqualTo("/v1/movieInfo" + "/" + movieId))
                .willReturn(aResponse()
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                        .withBodyFile("movieinfo.json")
                )
        );

        stubFor(get(urlPathEqualTo("/v1/reviews"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                        .withBodyFile("reviews.json")
                )
        );

        webTestClient
                .get()
                .uri("/v1/movies/{id}", movieId)
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(Movie.class)
                .consumeWith(response -> {
                    var movieInfo = response.getResponseBody();
                    assertThat(movieInfo.getMovieInfo().getMovieInfoId(), is("1"));
                    assertThat(movieInfo.getMovieInfo().getName(), is("Batman Begins"));

                    assertThat(movieInfo.getReviewList().size(), is(2));
                    assertThat(movieInfo.getReviewList().get(0).getMovieInfoId(), is("Awesome Movie"));
                });
    }
}
