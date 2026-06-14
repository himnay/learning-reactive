package com.reactivespring.controller;

import com.reactivespring.api.MoviesApi;
import com.reactivespring.client.MovieInfoRestClient;
import com.reactivespring.client.ReviewRestClient;
import com.reactivespring.entity.Movie;
import com.reactivespring.entity.MovieInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
        return movieInfoRestClient.retrieveMovieInfo(movieId)
                // Fan-out: fetch reviews in parallel once movie info arrives
                .flatMap(movieInfo ->
                        reviewRestClient.retrieveReviews(movieId)
                                .collectList()
                                .map(reviews -> ResponseEntity.ok(new Movie(movieInfo, reviews)))
                )
                .switchIfEmpty(Mono.just(ResponseEntity.<Movie>notFound()
                        .header("X-Reason", "No movie info found for id: " + movieId)
                        .build()));
    }

    @Override
    public Flux<MovieInfo> getMovieInfoStream() {
        return movieInfoRestClient.retrieveMovieInfoStream();
    }
}
