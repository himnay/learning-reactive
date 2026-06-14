package com.reactivespring.api;

import com.reactivespring.entity.Movie;
import com.reactivespring.entity.MovieInfo;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequestMapping("/v1/movies")
public interface MoviesApi {

    @GetMapping("/{id}")
    Mono<ResponseEntity<Movie>> retrieveMovieById(@PathVariable("id") String movieId);

    @GetMapping(value = "/stream", produces = MediaType.APPLICATION_NDJSON_VALUE)
    Flux<MovieInfo> getMovieInfoStream();
}
