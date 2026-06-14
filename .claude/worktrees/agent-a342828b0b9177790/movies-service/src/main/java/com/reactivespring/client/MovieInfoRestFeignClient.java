package com.reactivespring.client;

import com.reactivespring.entity.MovieInfo;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import reactivefeign.spring.config.ReactiveFeignClient;
import reactor.core.publisher.Mono;

@ReactiveFeignClient(value = "movie-service", url = "${movies.url.movieInfo}")
public interface MovieInfoRestFeignClient {

    @GetMapping("/v1/movieInfo/{movieInfoId}")
    Mono<MovieInfo> getMovieInfo(@PathVariable("movieInfoId") String movieInfoId);
}
