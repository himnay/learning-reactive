package com.reactivespring.router;

import com.reactivespring.entity.ReviewDocument;
import com.reactivespring.repository.ReviewRepository;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@SpringBootTest
@ActiveProfiles("test")
@AutoConfigureWebTestClient
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReviewInt {

    @Autowired
    private ReviewRepository reviewRepository;

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private WebClient.Builder webClientBuilder;

    @BeforeAll
    public void dbCleanup() {
        reviewRepository.deleteAll().block();
    }

    @BeforeEach
    public void init() {
        var reviewInfos = List.of(
                new ReviewDocument(null, 1l, "awesome movie", 9.0),
                new ReviewDocument(null, 2l, "awesome movie", 9.0),
                new ReviewDocument("abc", 3l, "awesome movie", 8.0)
        );

        // blockLast() as all apis are async and nonblocking. this will make sure all data are persisted before any test get execute
        // should be only used for testing only
        reviewRepository.saveAll(reviewInfos).blockLast();
    }

    @AfterEach
    public void tearDown() {
        reviewRepository.deleteAll().block();
    }

    @Test
    @DisplayName("test POST reviews")
    public void createReviewsTest() {
        var reviewInfo = new ReviewDocument(null, 1l, "awesome movie", 9.0);

        webTestClient
                .post()
                .uri("/v1/reviews")
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus()
                .isCreated()
                .expectBody(ReviewDocument.class)
                .consumeWith(i -> {
                    var reviewInfoResponse = i.getResponseBody();
                    assert reviewInfoResponse.getReviewId() != null;
                });
    }

    @Test
    @DisplayName("test GET list of reviews")
    public void getAllReviewsTest() {
        webTestClient.get()
                .uri("/v1/reviews")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBodyList(ReviewDocument.class)
                .hasSize(3)
                .consumeWith(i -> {
                    var reviews = i.getResponseBody();
                    assert reviews.size() == 3;
                });
    }

    @Test
    @DisplayName("test GET reviews by review id")
    public void getReviewsByIDTest() {
        webTestClient.get()
                .uri("/v1/reviews/{reviewId}", "abc")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBodyList(ReviewDocument.class)
                .hasSize(1)
                .consumeWith(i -> {
                    var reviews = i.getResponseBody();
                    assertThat(reviews.get(0).getReviewId(), is("abc"));
                });
    }

    @Test
    @DisplayName("test GET reviews by movieInfo id")
    public void getAllReviewsByMovieInfoIDTest() {
        var uri = UriComponentsBuilder.fromUriString("/v1/reviews")
                .queryParam("movieInfoId", 2l)
                .buildAndExpand().toUri();

        webTestClient.get()
                .uri(uri)
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .expectBodyList(ReviewDocument.class)
                .consumeWith(i -> {
                    var reviews = i.getResponseBody();
                    assert reviews.size() == 1;
                });
    }

    @Test
    @DisplayName("PUT review")
    public void upsertReviewTest() {
        var reviewInfo = new ReviewDocument(null, 1l, "awesome movie3", 9.0);

        webTestClient
                .put()
                .uri("/v1/reviews/{reviewId}", "abc")
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .expectBody(ReviewDocument.class)
                .consumeWith(i -> {
                    var movieInfoResponse = i.getResponseBody();
                    assert movieInfoResponse.getMovieInfoId() != null;
                    assertThat(movieInfoResponse.getComment(), is("awesome movie3"));
                });
    }

    @Test
    @DisplayName("PUT review")
    public void upsertReviewNotFoundTest() {
        var reviewInfo = new ReviewDocument("", 1l, "awesome movie4", 9.0);

        webTestClient
                .put()
                .uri("/v1/reviews/{reviewId}", "xyz")
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus()
                .isNotFound()
                .expectBody(String.class)
                .isEqualTo("Review not found for xyz");
    }

    @Test
    @DisplayName("DELETE review")
    public void deleteReviewTest() {
        webTestClient
                .delete()
                .uri("/v1/reviews/{reviewId}", "abc")
                .exchange()
                .expectStatus()
                .isNoContent()
                .expectBody(Void.class);
    }

}
