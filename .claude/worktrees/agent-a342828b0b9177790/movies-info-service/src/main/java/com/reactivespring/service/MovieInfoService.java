package com.reactivespring.service;

import com.reactivespring.entity.MovieInfoDocument;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface MovieInfoService {
    Mono<MovieInfoDocument> createMovieInfo(MovieInfoDocument movieInfoDocument);

    Flux<MovieInfoDocument> getAllMovies();

    Flux<MovieInfoDocument> getAllMoviesByYear(Integer year);

    Flux<MovieInfoDocument> getAllMoviesByName(String name);

    Mono<MovieInfoDocument> getMovieById(String movieInfoId);

    Mono<MovieInfoDocument> upsertMovieInfo(MovieInfoDocument movieInfoDocument, String movieInfoId);

    Mono<Void> deleteMovieInfo(String movieInfoId);
}
