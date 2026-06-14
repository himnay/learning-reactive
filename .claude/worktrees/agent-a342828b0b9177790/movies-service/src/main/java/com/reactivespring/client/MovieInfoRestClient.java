package com.reactivespring.client;

import com.reactivespring.entity.MovieInfo;
import com.reactivespring.exception.MoviesInfoClientException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class MovieInfoRestClient {

    @Value("${movies.url.movieInfo}")
    private String movieInfoUrl;

    private final WebClient webClient;
    private final MovieInfoRestFeignClient movieInfoRestFeignClient;

    public Mono<MovieInfo> retrieveMovieInfoUsingFeign(String movieId) {
        return movieInfoRestFeignClient.getMovieInfo(movieId);
    }

    public Mono<MovieInfo> retrieveMovieInfoUsingWebClient(String movieId) {
        var movieInfoUri = UriComponentsBuilder.fromHttpUrl(movieInfoUrl.concat("/{movieInfoId}"))
                .build()
                .toUriString();

        return webClient
                .get()
                .uri(movieInfoUri)
                .retrieve()
                .onStatus(HttpStatus::is4xxClientError, clientResponse -> {
                    if (clientResponse.statusCode().equals(HttpStatus.NOT_FOUND)) {
                        return Mono.error(new MoviesInfoClientException("There is no movie info found for the id : " + movieId, clientResponse.statusCode().value()));
                    }
                    return clientResponse
                            .bodyToMono(String.class)
                            .flatMap(responseMessage -> Mono.error(new MoviesInfoClientException(responseMessage, clientResponse.statusCode().value())));
                })
                .onStatus(HttpStatus::is5xxServerError, clientResponse -> clientResponse
                        .bodyToMono(String.class)
                        .flatMap(responseMessage -> Mono.error(new MoviesInfoClientException("Server Exception in MoviesInfoService " + responseMessage, clientResponse.statusCode().value()))))
                .bodyToMono(MovieInfo.class)
                .log();
    }

    public Flux<MovieInfo> retrieveMovieInfoStream() {
        var url = movieInfoUrl.concat("/stream");

        return webClient
                .get()
                .uri(url)
                .retrieve()
                .onStatus(HttpStatus::is4xxClientError, clientResponse -> {
                    log.info("Status code is : [{}]", clientResponse.statusCode());
                    return clientResponse
                            .bodyToMono(String.class)
                            .flatMap(responseMessage -> Mono.error(new MoviesInfoClientException(responseMessage, clientResponse.statusCode().value())));
                })
                .onStatus(HttpStatus::is5xxServerError, clientResponse -> clientResponse
                        .bodyToMono(String.class)
                        .flatMap(responseMessage -> Mono.error(new MoviesInfoClientException("Server Exception in MoviesInfoService " + responseMessage, clientResponse.statusCode().value()))))
                .bodyToFlux(MovieInfo.class)
                .log();
    }
}