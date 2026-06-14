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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@WebFluxTest(controllers = MovieInfoController.class)
public class MovieInfoControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private MovieInfoService movieInfoService;

    @Test
    @DisplayName("Approach 1 - test GET() flux rest api without reading response body")
    public void fluxRestApiWithBodyListSizeTest() {
        webTestClient
                .get()
                .uri("/v1/flux")
                .exchange()                     // exchange() actually makes the post() call
                .expectStatus()
                .is2xxSuccessful()
                .expectBodyList(Integer.class)
                .hasSize(3);
    }

    @Test
    @DisplayName("Approach 2 - test GET flux with its response body")
    public void fluxRestApiWithBodyFluxTest() {
        var responseBody = webTestClient
                .get()
                .uri("/v1/flux")
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .returnResult(Integer.class)
                .getResponseBody();

        StepVerifier.create(responseBody)
                .expectNext(1, 2, 3)
                .expectComplete();
    }

    @Test
    @DisplayName("Approach 3 - test GET flux with its response body")
    public void fluxRestApiWithBodyConsumeWithFluxTest() {
        webTestClient
                .get()
                .uri("/v1/flux")
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .expectBodyList(Integer.class)
                .consumeWith(i -> {
                    var responseBody = i.getResponseBody();
                    assert Objects.requireNonNull(responseBody).size() == 3;
                });
    }

    @Test
    @DisplayName("test GET() mono with its response body")
    public void monoRestApiWithBodyConsumeWithFluxTest() {
        webTestClient
                .get()
                .uri("/v1/mono")
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .expectBody(String.class)
                .consumeWith(i -> {
                    var responseBody = i.getResponseBody();
                    assertThat(responseBody, is("Hello World"));
                });
    }

    @Test
    @DisplayName("test GET flux stream")
    public void fluxStreamRestApiWithBodyTest() {
        var responseBody = webTestClient
                .get()
                .uri("/v1/stream")
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .returnResult(Integer.class)
                .getResponseBody();

        StepVerifier.create(responseBody)
                .expectNext(1, 2, 3)
                .thenCancel();
    }

    @Test
    @DisplayName("test GET getAllMovieInfos()")
    public void getAllMovieInfosTest() {
        var movieInfos = List.of(
                new MovieInfoDocument(null, "Batman Begins", 2005, List.of("Christian Bale", "Michael Cane"), LocalDate.parse("2005-06-15")),
                new MovieInfoDocument(null, "The Dark Knight", 2008, List.of("Christian Bale", "HeathLedger"), LocalDate.parse("2008-07-18")),
                new MovieInfoDocument("abc", "Dark Knight Rises", 2012, List.of("Christian Bale", "Tom Hardy"), LocalDate.parse("2012-07-20"))
        );

        when(movieInfoService.getAllMovies()).thenReturn(Flux.fromIterable(movieInfos));

        var responseBody = webTestClient
                .get()
                .uri("/v1/movieInfos")
                .exchange()                     // exchange() actually makes the post() call
                .expectStatus()
                .is2xxSuccessful()
                .expectBodyList(MovieInfoDocument.class)
                .hasSize(3);
    }

    @Test
    @DisplayName("test GET getMovieInfo()")
    public void getMovieInfoTest() {
        var movieInfo = Mono.just(
                new MovieInfoDocument("abc", "Dark Knight Rises", 2012, List.of("Christian Bale", "Tom Hardy"), LocalDate.parse("2012-07-20"))
        );

        when(movieInfoService.getMovieById(eq("abc"))).thenReturn(movieInfo);

        var responseBody = webTestClient
                .get()
                .uri("/v1/movieInfo/{movieInfoId}", "abc")
                .exchange()                     // exchange() actually makes the post() call
                .expectStatus()
                .is2xxSuccessful()
                .expectBody(MovieInfoDocument.class)
                .consumeWith(i -> {
                    var movieInfoResponseBody = i.getResponseBody();
                    assertThat(movieInfoResponseBody.getMovieInfoId(), is("abc"));
                    assertThat(movieInfoResponseBody.getName(), is("Dark Knight Rises"));
                });

        var movieInfoResponseBody = responseBody.returnResult().getResponseBody();

        assertThat(movieInfoResponseBody.getYear(), is(2012));
        assertThat(movieInfoResponseBody.getCast(), containsInAnyOrder("Tom Hardy", "Christian Bale"));
    }

    @Test
    @DisplayName("test POST createMovieInfo()")
    public void createMovieInfoTest() {
        var movieInfo = new MovieInfoDocument("abc", "Dark Knight Rises", 2012, List.of("Christian Bale", "Tom Hardy"), LocalDate.parse("2012-07-20"));

        when(movieInfoService.createMovieInfo(eq(movieInfo))).thenReturn(Mono.just(movieInfo));

        var responseBody = webTestClient
                .post()
                .uri("/v1/movieInfo")
                .bodyValue(movieInfo)
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .returnResult(MovieInfoDocument.class)
                .getResponseBody();

        StepVerifier.create(responseBody)
                .expectNextMatches(i -> i.getMovieInfoId().contains("abc"))
                .expectComplete()
                .verify();
    }

    @Test
    @DisplayName("test PUT upsertMovieInfo()")
    public void upsertMovieInfoTest() {
        var movieInfo = new MovieInfoDocument("abc", "Dark Knight Rises 2", 2012, List.of("Christian Bale", "Tom Hardy"), LocalDate.parse("2012-07-20"));

        when(movieInfoService.upsertMovieInfo(eq(movieInfo), eq("abc"))).thenReturn(Mono.just(movieInfo));

        var responseBody = webTestClient
                .put()
                .uri("/v1/movieInfo/{movieInfoId}", "abc")
                .bodyValue(movieInfo)
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .returnResult(MovieInfoDocument.class)
                .getResponseBody();

        StepVerifier.create(responseBody)
                .expectNextMatches(i -> i.getMovieInfoId().contains("abc"))
                .expectNextMatches(i -> i.getName().equals("Dark Knight Rises 2"))
                .expectComplete();
    }

    @Test
    @DisplayName("test DELETE deleteMovieInfo()")
    public void deleteMovieInfoTest() {
        Mono<Void> monoVoid = Mono.empty().then();

        when(movieInfoService.deleteMovieInfo(eq("abc"))).thenReturn(monoVoid);

        webTestClient
                .delete()
                .uri("/v1/movieInfo/{movieInfoId}", "abc")
                .exchange()
                .expectStatus()
                .isNoContent()
                .expectBody(Void.class);
    }

    @Test
    @DisplayName("validation for CREATE api")
    public void validateCreateMovieInfoTest() {
        var movieInfo = new MovieInfoDocument("abc", null, 0, List.of(""), LocalDate.parse("2012-07-20"));

        var responseBody = webTestClient
                .post()
                .uri("/v1/movieInfo")
                .bodyValue(movieInfo)
                .exchange()
                .expectStatus()
                .isBadRequest()
                .expectBody(String.class)
                .consumeWith(i -> {
                    String response = i.getResponseBody();
                    assertThat(response, is("MovieInfo.cast cannot be empty,MovieInfo.name cannot be empty,MovieInfo.year must be a positive no"));
                });
    }
}