package com.reactivespring.controller;

import com.reactivespring.api.MoviesApi;
import com.reactivespring.client.MovieInfoRestClient;
import com.reactivespring.client.ReviewRestClient;
import com.reactivespring.entity.Movie;
import com.reactivespring.entity.MovieInfo;
import com.reactivespring.entity.Review;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
public class MoviesController implements MoviesApi {

    private static final Logger log = LoggerFactory.getLogger(MoviesController.class);

    private final MovieInfoRestClient movieInfoRestClient;
    private final ReviewRestClient reviewRestClient;

    public MoviesController(MovieInfoRestClient movieInfoRestClient, ReviewRestClient reviewRestClient) {
        this.movieInfoRestClient = movieInfoRestClient;
        this.reviewRestClient = reviewRestClient;
    }

    @Override
    public Mono<ResponseEntity<Movie>> retrieveMovieById(String movieId) {
        // Fan-out: movie info and reviews depend only on the request's movieId,
        // so fetch both concurrently and join with zip instead of chaining flatMap.
        Mono<MovieInfo> movieInfoMono = movieInfoRestClient.retrieveMovieInfo(movieId);
        Mono<List<Review>> reviewsMono = reviewRestClient.retrieveReviews(movieId).collectList();

        return Mono.zip(movieInfoMono, reviewsMono)
                .map(tuple -> ResponseEntity.ok(new Movie(tuple.getT1(), tuple.getT2())))
                .switchIfEmpty(Mono.just(ResponseEntity.<Movie>notFound()
                        .header("X-Reason", "No movie info found for id: " + movieId)
                        .build()));
    }

    @Override
    public Flux<MovieInfo> getMovieInfoStream() {
        return movieInfoRestClient.retrieveMovieInfoStream();
    }
}
