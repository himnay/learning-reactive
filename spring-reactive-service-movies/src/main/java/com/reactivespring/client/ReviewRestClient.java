package com.reactivespring.client;

import com.reactivespring.entity.Review;
import com.reactivespring.exception.ReviewsClientException;
import com.reactivespring.exception.ReviewsServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.net.URI;
import java.time.Duration;

@Component
public class ReviewRestClient {

    private static final Logger log = LoggerFactory.getLogger(ReviewRestClient.class);

    private final WebClient webClient;
    private final String reviewsUrl;

    public ReviewRestClient(WebClient webClient,
                            @Value("${movies.url.reviews}") String reviewsUrl) {
        this.webClient = webClient;
        this.reviewsUrl = reviewsUrl;
    }

    /** Returns the retrieve reviews. */
    public Flux<Review> retrieveReviews(String movieId) {
        URI reviewUri = UriComponentsBuilder.fromUriString(reviewsUrl)
                .queryParam("movieInfoId", movieId)
                .build()
                .toUri();

        return webClient
                .get()
                .uri(reviewUri)
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, response -> {
                    log.warn("4xx from ReviewService [movieId={}]: {}", movieId, response.statusCode());
                    if (response.statusCode().equals(HttpStatus.NOT_FOUND)) {
                        // Mono.empty() here would mean "not an error" and WebClient would then try to
                        // decode the plain-text 404 body as Review (500). Signal it, resume below.
                        return response.createException();
                    }
                    return response.bodyToMono(String.class)
                            .flatMap(msg -> Mono.error(new ReviewsClientException(msg)));
                })
                .onStatus(HttpStatusCode::is5xxServerError, response ->
                        response.bodyToMono(String.class)
                                .flatMap(msg -> Mono.error(new ReviewsServerException(
                                        "Server error in ReviewService: " + msg))))
                .bodyToFlux(Review.class)
                // no reviews yet is a normal state for the aggregate, not a failure
                .onErrorResume(WebClientResponseException.NotFound.class, ex -> Flux.empty())
                // Retry up to 3 times on server errors with exponential backoff
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .filter(ex -> ex instanceof ReviewsServerException)
                        .onRetryExhaustedThrow((spec, signal) ->
                                signal.failure()))
                .log();
    }
}
