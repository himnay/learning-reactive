package com.reactivespring.handler;

import com.reactivespring.entity.ReviewDocument;
import com.reactivespring.exception.ReviewNotFoundException;
import com.reactivespring.repository.ReviewRepository;
import com.reactivespring.validator.ReviewValidator;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Component
@RequiredArgsConstructor
public class ReviewHandler {

    private Sinks.Many<ReviewDocument> reviewInfoSinks = Sinks.many().replay().latest();

    private final ReviewValidator reviewValidator;
    private final ReviewRepository reviewRepository;

    public Mono<ServerResponse> addReview(ServerRequest request) {
        return request.bodyToMono(ReviewDocument.class)
                .doOnNext(reviewValidator::validator)
                .flatMap(reviewRepository::save)
                .doOnNext(reviewInfoSinks::tryEmitNext)  // for streaming
                .flatMap(ServerResponse.status(HttpStatus.CREATED)::bodyValue);
    }

    public Mono<ServerResponse> getReviews(ServerRequest request) {
        var movieInfoId = request.queryParam("movieInfoId");
        var reviewId = request.pathVariables().get("reviewId");
        if (movieInfoId.isPresent()) {
            var reviews = reviewRepository.findByMovieInfoId(Long.valueOf(movieInfoId.get()))
                    .switchIfEmpty(Mono.error(new ReviewNotFoundException("Review Not found for movie " + movieInfoId)));
            return ServerResponse.ok().body(reviews, ReviewDocument.class);

        } else if (StringUtils.hasLength(reviewId)) {
            return reviewRepository.findById(reviewId)
                    .flatMap(ServerResponse.ok()::bodyValue)
                    .switchIfEmpty(Mono.error(new ReviewNotFoundException("Review not found for " + reviewId)));
        } else {
            var reviews = reviewRepository.findAll();
            return ServerResponse.ok().body(reviews, ReviewDocument.class);
        }
    }

    public Mono<ServerResponse> upsertReview(ServerRequest request) {
        var reviewId = request.pathVariable("reviewId");
        var reviewDocument = reviewRepository.findById(reviewId)
                .switchIfEmpty(Mono.error(new ReviewNotFoundException("Review not found for " + reviewId)));
        return reviewDocument
                .flatMap(existingReviewDocument -> request.bodyToMono(ReviewDocument.class)
                        .map(requestReviewDocument -> {
                            existingReviewDocument.setComment(requestReviewDocument.getComment());
                            existingReviewDocument.setRating(requestReviewDocument.getRating());
                            return existingReviewDocument;
                        })
                        .flatMap(reviewRepository::save)
                        .flatMap(ServerResponse.ok()::bodyValue)
                ).switchIfEmpty(ServerResponse.notFound().build())
                .log();
    }

    public Mono<ServerResponse> deleteReview(ServerRequest request) {
        return reviewRepository.deleteById(request.pathVariable("reviewId"))
                .then(ServerResponse.noContent().build());
    }

    public Mono<ServerResponse> getReviewsStream(ServerRequest request) {
        return ServerResponse
                .ok()
                .contentType(MediaType.APPLICATION_NDJSON)
                .body(reviewInfoSinks.asFlux(), ReviewDocument.class)
                .log();
    }
}