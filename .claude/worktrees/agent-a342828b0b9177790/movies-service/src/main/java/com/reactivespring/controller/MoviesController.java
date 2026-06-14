package com.reactivespring.controller;

import com.reactivespring.api.MoviesApi;
import com.reactivespring.client.MovieInfoRestClient;
import com.reactivespring.client.MovieInfoRestFeignClient;
import com.reactivespring.client.ReviewRestClient;
import com.reactivespring.client.ReviewRestFeignClient;
import com.reactivespring.entity.Movie;
import com.reactivespring.entity.MovieInfo;
import com.reactivespring.entity.Review;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
public class MoviesController implements MoviesApi {

    private final MovieInfoRestClient moviesInfoRestClient;
    private final ReviewRestClient reviewRestClient;
    private final MovieInfoRestFeignClient movieInfoRestFeignClient;
    private final ReviewRestFeignClient reviewRestFeignClient;

    @Override
    public Mono<ResponseEntity<Movie>> retrieveMovieById(String movieId) {
        return movieInfoRestFeignClient.getMovieInfo(movieId)
                .flatMap(movieInfo -> {
                    Mono<List<Review>> listMono = reviewRestFeignClient.getReview(movieId)
                            .map(i -> new Review(i.getReviewId(), i.getMovieInfoId(), i.getComment(), i.getRating()))
                            .collectList()
                            .flatMap(i -> new Movie(movieInfo, i))
                }).switchIfEmpty(Mono.just(ResponseEntity
                        .notFound()
                        .header("X-Reason", "movie info id cannot be found")
                        .build()));
    }

    @Override
    public Flux<MovieInfo> getMovieInfoStream() {
        return moviesInfoRestClient.retrieveMovieInfoStream();
    }
}
