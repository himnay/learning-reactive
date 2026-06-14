package com.reactivespring.client;

import com.reactivespring.entity.MovieInfo;
import com.reactivespring.exception.MoviesInfoClientException;
import com.reactivespring.exception.MoviesInfoServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Component
public class MovieInfoRestClient {

    private static final Logger log = LoggerFactory.getLogger(MovieInfoRestClient.class);

    private final WebClient webClient;
    private final String movieInfoUrl;

    public MovieInfoRestClient(WebClient webClient,
                               @Value("${movies.url.movieInfo}") String movieInfoUrl) {
        this.webClient = webClient;
        this.movieInfoUrl = movieInfoUrl;
    }

    public Mono<MovieInfo> retrieveMovieInfo(String movieId) {
        return webClient
                .get()
                .uri(movieInfoUrl + "/{movieInfoId}", movieId)
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, response -> {
                    log.warn("4xx from MovieInfoService [movieId={}]: {}", movieId, response.statusCode());
                    if (response.statusCode().equals(HttpStatus.NOT_FOUND)) {
                        return Mono.error(new MoviesInfoClientException(
                                "No movie info found for id: " + movieId,
                                response.statusCode().value()));
                    }
                    return response.bodyToMono(String.class)
                            .flatMap(msg -> Mono.error(new MoviesInfoClientException(
                                    msg, response.statusCode().value())));
                })
                .onStatus(HttpStatusCode::is5xxServerError, response ->
                        response.bodyToMono(String.class)
                                .flatMap(msg -> Mono.error(new MoviesInfoServerException(
                                        "Server error in MovieInfoService: " + msg))))
                .bodyToMono(MovieInfo.class)
                // Retry up to 3 times on server errors with exponential backoff
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .filter(ex -> ex instanceof MoviesInfoServerException)
                        .onRetryExhaustedThrow((spec, signal) ->
                                signal.failure()))
                .log();
    }

    public Flux<MovieInfo> retrieveMovieInfoStream() {
        return webClient
                .get()
                .uri(movieInfoUrl + "/stream")
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, response ->
                        response.bodyToMono(String.class)
                                .flatMap(msg -> Mono.error(new MoviesInfoClientException(
                                        msg, response.statusCode().value()))))
                .onStatus(HttpStatusCode::is5xxServerError, response ->
                        response.bodyToMono(String.class)
                                .flatMap(msg -> Mono.error(new MoviesInfoServerException(
                                        "Server error in MovieInfoService: " + msg))))
                .bodyToFlux(MovieInfo.class)
                // Repeat indefinitely for SSE — resubscribe on server-side completion
                .repeat()
                .log();
    }
}
