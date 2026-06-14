package com.reactivespring.handler;

import com.reactivespring.entity.ReviewDocument;
import com.reactivespring.exception.ReviewNotFoundException;
import com.reactivespring.repository.ReviewRepository;
import com.reactivespring.validator.ReviewValidator;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Component
public class ReviewHandler {

    // Sinks.many().replay().latest() — hot publisher replaying only the latest event to new subscribers
    private final Sinks.Many<ReviewDocument> reviewInfoSinks = Sinks.many().replay().latest();

    private final ReviewValidator reviewValidator;
    private final ReviewRepository reviewRepository;

    public ReviewHandler(ReviewValidator reviewValidator, ReviewRepository reviewRepository) {
        this.reviewValidator = reviewValidator;
        this.reviewRepository = reviewRepository;
    }

    public Mono<ServerResponse> addReview(ServerRequest request) {
        return request.bodyToMono(ReviewDocument.class)
                .doOnNext(reviewValidator::validate)
                .flatMap(reviewRepository::save)
                .doOnNext(reviewInfoSinks::tryEmitNext)
                .flatMap(ServerResponse.status(HttpStatus.CREATED)::bodyValue);
    }

    public Mono<ServerResponse> getReviews(ServerRequest request) {
        var movieInfoId = request.queryParam("movieInfoId");
        var reviewId = request.pathVariables().get("reviewId");
        if (movieInfoId.isPresent()) {
            var reviews = reviewRepository.findByMovieInfoId(Long.valueOf(movieInfoId.get()))
                    .switchIfEmpty(Mono.error(new ReviewNotFoundException("Review not found for movie " + movieInfoId.get())));
            return ServerResponse.ok().body(reviews, ReviewDocument.class);
        } else if (StringUtils.hasLength(reviewId)) {
            return reviewRepository.findById(reviewId)
                    .flatMap(ServerResponse.ok()::bodyValue)
                    .switchIfEmpty(Mono.error(new ReviewNotFoundException("Review not found for " + reviewId)));
        }
        return ServerResponse.ok().body(reviewRepository.findAll(), ReviewDocument.class);
    }

    public Mono<ServerResponse> upsertReview(ServerRequest request) {
        var reviewId = request.pathVariable("reviewId");
        return reviewRepository.findById(reviewId)
                .switchIfEmpty(Mono.error(new ReviewNotFoundException("Review not found for " + reviewId)))
                .flatMap(existing -> request.bodyToMono(ReviewDocument.class)
                        .map(incoming -> new ReviewDocument(
                                existing.reviewId(),
                                existing.movieInfoId(),
                                incoming.comment(),
                                incoming.rating()))
                        .flatMap(reviewRepository::save)
                        .flatMap(ServerResponse.ok()::bodyValue))
                .log();
    }

    public Mono<ServerResponse> deleteReview(ServerRequest request) {
        return reviewRepository.deleteById(request.pathVariable("reviewId"))
                .then(ServerResponse.noContent().build());
    }

    public Mono<ServerResponse> getReviewsStream(ServerRequest request) {
        return ServerResponse.ok()
                .contentType(MediaType.APPLICATION_NDJSON)
                .body(reviewInfoSinks.asFlux(), ReviewDocument.class)
                .log();
    }
}
