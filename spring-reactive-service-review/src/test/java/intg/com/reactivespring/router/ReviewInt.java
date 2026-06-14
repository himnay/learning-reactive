package com.reactivespring.router;

import com.reactivespring.entity.ReviewDocument;
import com.reactivespring.repository.ReviewRepository;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.util.UriComponentsBuilder;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
@AutoConfigureWebTestClient
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReviewInt {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:7");

    @Autowired
    private ReviewRepository reviewRepository;

    @Autowired
    private WebTestClient webTestClient;

    @BeforeAll
    public void dbCleanup() {
        reviewRepository.deleteAll().block();
    }

    @BeforeEach
    public void init() {
        var reviewInfos = List.of(
                new ReviewDocument(null, 1L, "awesome movie", 9.0),
                new ReviewDocument(null, 2L, "awesome movie", 9.0),
                new ReviewDocument("abc", 3L, "awesome movie", 8.0)
        );
        reviewRepository.saveAll(reviewInfos).blockLast();
    }

    @AfterEach
    public void tearDown() {
        reviewRepository.deleteAll().block();
    }

    @Test
    @DisplayName("POST review")
    public void createReviewsTest() {
        var reviewInfo = new ReviewDocument(null, 1L, "awesome movie", 9.0);

        webTestClient.post()
                .uri("/v1/reviews")
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ReviewDocument.class)
                .consumeWith(result -> {
                    var response = result.getResponseBody();
                    assertThat(response).isNotNull();
                    assertThat(response.reviewId()).isNotNull();
                });
    }

    @Test
    @DisplayName("GET all reviews")
    public void getAllReviewsTest() {
        webTestClient.get()
                .uri("/v1/reviews")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(ReviewDocument.class)
                .hasSize(3)
                .consumeWith(result -> assertThat(result.getResponseBody()).hasSize(3));
    }

    @Test
    @DisplayName("GET review by id")
    public void getReviewsByIDTest() {
        webTestClient.get()
                .uri("/v1/reviews/{reviewId}", "abc")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(ReviewDocument.class)
                .hasSize(1)
                .consumeWith(result -> {
                    var reviews = result.getResponseBody();
                    assertThat(reviews.get(0).reviewId()).isEqualTo("abc");
                });
    }

    @Test
    @DisplayName("GET reviews by movieInfoId")
    public void getAllReviewsByMovieInfoIDTest() {
        var uri = UriComponentsBuilder.fromUriString("/v1/reviews")
                .queryParam("movieInfoId", 2L)
                .buildAndExpand().toUri();

        webTestClient.get()
                .uri(uri)
                .exchange()
                .expectStatus().is2xxSuccessful()
                .expectBodyList(ReviewDocument.class)
                .hasSize(1)
                .consumeWith(result -> {
                    var reviews = result.getResponseBody();
                    assertThat(reviews).hasSize(1);
                    assertThat(reviews.get(0).movieInfoId()).isEqualTo(2L);
                });
    }

    @Test
    @DisplayName("PUT review - update existing")
    public void upsertReviewTest() {
        var reviewInfo = new ReviewDocument(null, 1L, "awesome movie3", 9.0);

        webTestClient.put()
                .uri("/v1/reviews/{reviewId}", "abc")
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus().is2xxSuccessful()
                .expectBody(ReviewDocument.class)
                .consumeWith(result -> {
                    var response = result.getResponseBody();
                    assertThat(response).isNotNull();
                    assertThat(response.reviewId()).isNotNull();
                    assertThat(response.comment()).isEqualTo("awesome movie3");
                });
    }

    @Test
    @DisplayName("PUT review - not found")
    public void upsertReviewNotFoundTest() {
        var reviewInfo = new ReviewDocument("", 1L, "awesome movie4", 9.0);

        webTestClient.put()
                .uri("/v1/reviews/{reviewId}", "xyz")
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody(String.class)
                .isEqualTo("Review not found for xyz");
    }

    @Test
    @DisplayName("DELETE review")
    public void deleteReviewTest() {
        webTestClient.delete()
                .uri("/v1/reviews/{reviewId}", "abc")
                .exchange()
                .expectStatus().isNoContent()
                .expectBody(Void.class);
    }
}
