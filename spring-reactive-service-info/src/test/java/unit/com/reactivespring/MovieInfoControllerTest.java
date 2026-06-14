package com.reactivespring;

import com.reactivespring.entity.MovieInfoDocument;
import com.reactivespring.router.MovieInfoController;
import com.reactivespring.service.MovieInfoService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@WebFluxTest(controllers = MovieInfoController.class)
class MovieInfoControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private MovieInfoService movieInfoService;

    @Test
    @DisplayName("GET /v1/flux — returns 3 elements")
    void fluxRestApiWithBodyListSizeTest() {
        webTestClient.get().uri("/v1/flux").exchange()
                .expectStatus().is2xxSuccessful()
                .expectBodyList(Integer.class).hasSize(3);
    }

    @Test
    @DisplayName("GET /v1/flux — StepVerifier approach")
    void fluxRestApiWithBodyFluxTest() {
        var responseBody = webTestClient.get().uri("/v1/flux").exchange()
                .expectStatus().is2xxSuccessful()
                .returnResult(Integer.class).getResponseBody();
        StepVerifier.create(responseBody).expectNext(1, 2, 3).expectComplete();
    }

    @Test
    @DisplayName("GET /v1/flux — consumeWith approach")
    void fluxRestApiWithBodyConsumeWithTest() {
        webTestClient.get().uri("/v1/flux").exchange()
                .expectStatus().is2xxSuccessful()
                .expectBodyList(Integer.class)
                .consumeWith(resp -> assertThat(Objects.requireNonNull(resp.getResponseBody())).hasSize(3));
    }

    @Test
    @DisplayName("GET /v1/mono — returns Hello World")
    void monoRestApiTest() {
        webTestClient.get().uri("/v1/mono").exchange()
                .expectStatus().is2xxSuccessful()
                .expectBody(String.class)
                .consumeWith(resp -> assertThat(resp.getResponseBody()).isEqualTo("Hello World"));
    }

    @Test
    @DisplayName("GET /v1/movieInfos — returns mocked list")
    void getAllMovieInfosTest() {
        var movieInfos = List.of(
                new MovieInfoDocument(null, "Batman Begins", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15")),
                new MovieInfoDocument(null, "The Dark Knight", 2008, List.of("Christian Bale", "HeathLedger"), LocalDate.parse("2008-07-18")),
                new MovieInfoDocument("abc", "Dark Knight Rises", 2012, List.of("Christian Bale", "Tom Hardy"), LocalDate.parse("2012-07-20"))
        );
        when(movieInfoService.getAllMovies()).thenReturn(Flux.fromIterable(movieInfos));
        webTestClient.get().uri("/v1/movieInfos").exchange()
                .expectStatus().is2xxSuccessful()
                .expectBodyList(MovieInfoDocument.class).hasSize(3);
    }

    @Test
    @DisplayName("GET /v1/movieInfo/{id} — returns correct document")
    void getMovieInfoTest() {
        var movieInfo = new MovieInfoDocument("abc", "Dark Knight Rises", 2012, List.of("Christian Bale", "Tom Hardy"), LocalDate.parse("2012-07-20"));
        when(movieInfoService.getMovieById(eq("abc"))).thenReturn(Mono.just(movieInfo));

        webTestClient.get().uri("/v1/movieInfo/{movieInfoId}", "abc")
                .exchange()
                .expectStatus().is2xxSuccessful()
                .expectBody(MovieInfoDocument.class)
                .consumeWith(resp -> {
                    var doc = resp.getResponseBody();
                    assertThat(doc.movieInfoId()).isEqualTo("abc");
                    assertThat(doc.name()).isEqualTo("Dark Knight Rises");
                    assertThat(doc.year()).isEqualTo(2012);
                    assertThat(doc.cast()).containsExactlyInAnyOrder("Tom Hardy", "Christian Bale");
                });
    }

    @Test
    @DisplayName("POST /v1/movieInfo — creates movie via service mock")
    void createMovieInfoTest() {
        var movieInfo = new MovieInfoDocument("abc", "Dark Knight Rises", 2012, List.of("Christian Bale", "Tom Hardy"), LocalDate.parse("2012-07-20"));
        when(movieInfoService.createMovieInfo(eq(movieInfo))).thenReturn(Mono.just(movieInfo));

        StepVerifier.create(webTestClient.post().uri("/v1/movieInfo")
                        .bodyValue(movieInfo).exchange()
                        .expectStatus().is2xxSuccessful()
                        .returnResult(MovieInfoDocument.class).getResponseBody())
                .expectNextMatches(doc -> doc.movieInfoId().contains("abc"))
                .verifyComplete();
    }

    @Test
    @DisplayName("PUT /v1/movieInfo/{id} — updates via service mock")
    void upsertMovieInfoTest() {
        var movieInfo = new MovieInfoDocument("abc", "Dark Knight Rises 2", 2012, List.of("Christian Bale", "Tom Hardy"), LocalDate.parse("2012-07-20"));
        when(movieInfoService.upsertMovieInfo(eq(movieInfo), eq("abc"))).thenReturn(Mono.just(movieInfo));

        StepVerifier.create(webTestClient.put().uri("/v1/movieInfo/{movieInfoId}", "abc")
                        .bodyValue(movieInfo).exchange()
                        .expectStatus().is2xxSuccessful()
                        .returnResult(MovieInfoDocument.class).getResponseBody())
                .expectNextMatches(doc -> doc.movieInfoId().contains("abc"))
                .expectNextMatches(doc -> doc.name().equals("Dark Knight Rises 2"))
                .expectComplete();
    }

    @Test
    @DisplayName("DELETE /v1/movieInfo/{id} — delegates to service")
    void deleteMovieInfoTest() {
        when(movieInfoService.deleteMovieInfo(eq("abc"))).thenReturn(Mono.empty().then());
        webTestClient.delete().uri("/v1/movieInfo/{movieInfoId}", "abc")
                .exchange().expectStatus().isNoContent().expectBody(Void.class);
    }

    @Test
    @DisplayName("POST validation — bad request on invalid body")
    void validateCreateMovieInfoTest() {
        var invalid = new MovieInfoDocument("abc", null, 0, List.of(""), LocalDate.parse("2012-07-20"));
        webTestClient.post().uri("/v1/movieInfo")
                .bodyValue(invalid).exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class)
                .consumeWith(resp -> assertThat(resp.getResponseBody())
                        .isEqualTo("MovieInfo.cast cannot be empty,MovieInfo.name cannot be empty,MovieInfo.year must be a positive no"));
    }
}
