import com.reactivespring.entity.ReviewDocument;
import com.reactivespring.handler.GlobalExceptionHandler;
import com.reactivespring.handler.ReviewHandler;
import com.reactivespring.repository.ReviewRepository;
import com.reactivespring.router.ReviewRouter;
import com.reactivespring.validator.ReviewValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@WebFluxTest
@AutoConfigureWebTestClient
@ContextConfiguration(classes = {ReviewRouter.class, ReviewHandler.class, ReviewValidator.class, GlobalExceptionHandler.class})
public class ReviewTest {

    @MockBean
    private ReviewRepository reviewRepository;

    @Autowired
    private WebTestClient webTestClient;

    @Test
    @DisplayName("POST review")
    public void createReviewsTest() {
        var reviewId = UUID.randomUUID().toString();
        var reviewInfo = new ReviewDocument(reviewId, 1L, "awesome movie", 9.0);

        when(reviewRepository.save(reviewInfo)).thenReturn(Mono.just(reviewInfo));

        webTestClient.post()
                .uri("/v1/reviews")
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ReviewDocument.class)
                .consumeWith(result -> {
                    var response = result.getResponseBody();
                    assertThat(response).isNotNull();
                    assertThat(response.reviewId()).isEqualTo(reviewId);
                });
    }

    @Test
    @DisplayName("POST review with validation errors")
    public void createReviewsWitWrongReviewDocumentTest() {
        var reviewId = UUID.randomUUID().toString();
        var reviewInfo = new ReviewDocument(reviewId, null, "awesome movie", -9.0);

        webTestClient.post()
                .uri("/v1/reviews")
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class)
                .isEqualTo("Review.movieInfoId cannot be null,rating.negative : please pass a non-negative value");
    }

    @Test
    @DisplayName("GET all reviews")
    public void getAllReviewsTest() {
        var reviewInfos = List.of(
                new ReviewDocument(null, 1L, "awesome movie", 9.0),
                new ReviewDocument(null, 2L, "awesome movie", 9.0),
                new ReviewDocument("abc", 3L, "awesome movie", 8.0)
        );

        when(reviewRepository.findAll()).thenReturn(Flux.fromIterable(reviewInfos));

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
        var review = new ReviewDocument("abc", 3L, "awesome movie", 8.0);

        when(reviewRepository.findById("abc")).thenReturn(Mono.just(review));

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
        var review = new ReviewDocument("abc", 3L, "awesome movie", 8.0);

        var uri = UriComponentsBuilder.fromUriString("/v1/reviews")
                .queryParam("movieInfoId", 3L)
                .buildAndExpand().toUri();

        when(reviewRepository.findByMovieInfoId(3L)).thenReturn(Flux.just(review));

        webTestClient.get()
                .uri(uri)
                .exchange()
                .expectStatus().is2xxSuccessful()
                .expectBodyList(ReviewDocument.class)
                .hasSize(1)
                .consumeWith(result -> {
                    var reviews = result.getResponseBody();
                    assertThat(reviews.get(0).comment()).isEqualTo("awesome movie");
                    assertThat(reviews.get(0).rating()).isEqualTo(8.0);
                });
    }

    @Test
    @DisplayName("PUT review - update existing")
    public void upsertReviewTest() {
        var reviewId = UUID.randomUUID().toString();
        var reviewInfo = new ReviewDocument(reviewId, 1L, "awesome movie", 9.0);

        when(reviewRepository.findById(reviewId)).thenReturn(Mono.just(reviewInfo));
        when(reviewRepository.save(reviewInfo)).thenReturn(Mono.just(reviewInfo));

        webTestClient.put()
                .uri("/v1/reviews/{reviewId}", reviewId)
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus().is2xxSuccessful()
                .expectBody(ReviewDocument.class)
                .consumeWith(result -> {
                    var response = result.getResponseBody();
                    assertThat(response).isNotNull();
                    assertThat(response.reviewId()).isEqualTo(reviewId);
                    assertThat(response.comment()).isEqualTo("awesome movie");
                });
    }

    @Test
    @DisplayName("PUT review - not found")
    public void upsertReviewNotFoundTest() {
        var reviewId = UUID.randomUUID().toString();
        var reviewInfo = new ReviewDocument(reviewId, 1L, "awesome movie", 9.0);

        when(reviewRepository.findById(reviewId)).thenReturn(Mono.empty());

        webTestClient.put()
                .uri("/v1/reviews/{reviewId}", reviewId)
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    @DisplayName("DELETE review")
    public void deleteReviewTest() {
        var reviewId = UUID.randomUUID().toString();

        when(reviewRepository.deleteById(reviewId)).thenReturn(Mono.empty());

        webTestClient.delete()
                .uri("/v1/reviews/{reviewId}", reviewId)
                .exchange()
                .expectStatus().isNoContent()
                .expectBody(Void.class);
    }
}
