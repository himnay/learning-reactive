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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
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
    @DisplayName("test POST reviews")
    public void createReviewsTest() {
        var reviewId = UUID.randomUUID().toString();
        var reviewInfo = new ReviewDocument(reviewId, 1l, "awesome movie", 9.0);

        when(reviewRepository.save(reviewInfo)).thenReturn(Mono.just(reviewInfo));

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
                    assertThat(reviewInfoResponse.getReviewId(), is(reviewId));
                });
    }

    @Test
    @DisplayName("test POST reviews with validation")
    public void createReviewsWitWrongReviewDocumentTest() {
        var reviewId = UUID.randomUUID().toString();
        var reviewInfo = new ReviewDocument(reviewId, null, "awesome movie", -9.0);

        webTestClient
                .post()
                .uri("/v1/reviews")
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus()
                .isBadRequest()
                .expectBody(String.class)
                .isEqualTo("Review.movieInfoId cannot be null,rating.negative : please pass a non-negative value");
    }

    @Test
    @DisplayName("test GET list of reviews")
    public void getAllReviewsTest() {
        var reviewInfos = List.of(
                new ReviewDocument(null, 1l, "awesome movie", 9.0),
                new ReviewDocument(null, 2l, "awesome movie", 9.0),
                new ReviewDocument("abc", 3l, "awesome movie", 8.0)
        );

        when(reviewRepository.findAll()).thenReturn(Flux.fromIterable(reviewInfos));

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
        var review = new ReviewDocument("abc", 3l, "awesome movie", 8.0);

        when(reviewRepository.findById("abc")).thenReturn(Mono.just(review));

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
        var review = new ReviewDocument("abc", 3l, "awesome movie", 8.0);

        var uri = UriComponentsBuilder.fromUriString("/v1/reviews")
                .queryParam("movieInfoId", 3l)
                .buildAndExpand().toUri();

        when(reviewRepository.findByMovieInfoId(3l)).thenReturn(Flux.just(review));

        webTestClient.get()
                .uri(uri)
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .expectBodyList(ReviewDocument.class)
                .hasSize(1)
                .consumeWith(i -> {
                    var reviews = i.getResponseBody();
                    assertThat(reviews.get(0).getComment(), is("awesome movie"));
                    assertThat(reviews.get(0).getRating(), is(8.0));
                });
    }

    @Test
    @DisplayName("PUT review")
    public void upsertReviewTest() {
        var reviewId = UUID.randomUUID().toString();
        var reviewInfo = new ReviewDocument(reviewId, 1l, "awesome movie", 9.0);

        when(reviewRepository.findById(reviewId)).thenReturn(Mono.just(reviewInfo));
        when(reviewRepository.save(reviewInfo)).thenReturn(Mono.just(reviewInfo));

        webTestClient
                .put()
                .uri("/v1/reviews/{reviewId}", reviewId)
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus()
                .is2xxSuccessful()
                .expectBody(ReviewDocument.class)
                .consumeWith(i -> {
                    var movieInfoResponse = i.getResponseBody();
                    assertThat(movieInfoResponse.getReviewId(), is(reviewId));
                    assertThat(movieInfoResponse.getComment(), is("awesome movie"));
                });
    }

    @Test
    @DisplayName("PUT review")
    public void upsertReviewNotFoundTest() {
        var reviewId = UUID.randomUUID().toString();
        var reviewInfo = new ReviewDocument(reviewId, 1l, "awesome movie", 9.0);

        when(reviewRepository.findById(reviewId)).thenReturn(Mono.empty());

        webTestClient
                .put()
                .uri("/v1/reviews/{reviewId}", reviewId)
                .bodyValue(reviewInfo)
                .exchange()
                .expectStatus()
                .isNotFound();
    }

    @Test
    @DisplayName("DELETE review")
    public void deleteReviewTest() {
        var reviewId = UUID.randomUUID().toString();
        Mono<Void> monoVoid = Mono.empty().then();

        when(reviewRepository.deleteById(reviewId)).thenReturn(monoVoid);

        webTestClient
                .delete()
                .uri("/v1/reviews/{reviewId}", reviewId)
                .exchange()
                .expectStatus()
                .isNoContent()
                .expectBody(Void.class);
    }

}
