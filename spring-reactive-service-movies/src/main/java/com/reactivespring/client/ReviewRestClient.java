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
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;

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
                        return Mono.empty();
                    }
                    return response.bodyToMono(String.class)
                            .flatMap(msg -> Mono.error(new ReviewsClientException(msg)));
                })
                .onStatus(HttpStatusCode::is5xxServerError, response ->
                        response.bodyToMono(String.class)
                                .flatMap(msg -> Mono.error(new ReviewsServerException(
                                        "Server error in ReviewService: " + msg))))
                .bodyToFlux(Review.class)
                .log();
    }
}
