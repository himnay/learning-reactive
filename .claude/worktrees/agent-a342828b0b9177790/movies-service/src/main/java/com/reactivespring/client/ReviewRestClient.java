package com.reactivespring.client;

import com.reactivespring.entity.Review;
import com.reactivespring.exception.ReviewsClientException;
import com.reactivespring.exception.ReviewsServerException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReviewRestClient {

    @Value("${movies.url.reviews}")
    private String reviewsUrl;

    private final WebClient webClient;

    public Flux<Review> retrieveReviews(String movieId) {
        URI reviewUrl = UriComponentsBuilder.fromHttpUrl(reviewsUrl)
                .queryParam("movieInfoId", movieId)
                .build()
                .toUri();

        return webClient
                .get()
                .uri("http://localhost:8081/v1/reviews?movieInfoId=1")
                .retrieve()
                .onStatus(HttpStatus::is4xxClientError, clientResponse -> {
                    log.error("Status code is ", +clientResponse.statusCode().value());
                    if (clientResponse.statusCode().is4xxClientError()) {
                        return Mono.empty();
                    }
                    return clientResponse
                            .bodyToMono(String.class)
                            .flatMap(responses -> Mono.error(new ReviewsClientException(responses)));
                })
                .onStatus(HttpStatus::is5xxServerError, clientResponse -> {
                    log.error("Status code is " + clientResponse.statusCode().value());
                    return clientResponse
                            .bodyToMono(String.class)
                            .flatMap(responses -> Mono.error(new ReviewsServerException("Server exception in ReviewService " + responses)));
                })
                .bodyToFlux(Review.class)
                .log();
    }
}
