package com.reactivespring.router;

import com.reactivespring.api.MovieInfoApi;
import com.reactivespring.entity.MovieInfoDocument;
import com.reactivespring.enums.MovieInfoFilterCriteria;
import com.reactivespring.service.MovieInfoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Map;

@Slf4j
@RestController
@RequiredArgsConstructor
public class MovieInfoController implements MovieInfoApi {

    private Sinks.Many<MovieInfoDocument> movieInfoSinks = Sinks.many().replay().all();

    private final MovieInfoService movieInfoService;

    @Override
    public Flux<Integer> nonBlockingFlux() {
        return Flux.just(1, 2, 3).delayElements(Duration.ofSeconds(1)).log();
    }

    @Override
    public Mono<String> nonBlockingMono() {
        return Mono.just("Hello World").log();
    }

    @Override
    public Flux<Long> nonBlockingFluxStream() {
        return Flux.interval(Duration.ofSeconds(1)).log();
    }

    @Override
    public Flux<MovieInfoDocument> getMovieInfoStream() {
        return movieInfoSinks.asFlux();
    }

    @Override
    public Mono<MovieInfoDocument> createMovieInfo(MovieInfoDocument movieInfoDocument) {
        return movieInfoService.createMovieInfo(movieInfoDocument)
                .doOnNext(movieInfoSinks::tryEmitNext)
                .log();
    }

    @Override
    public Flux<MovieInfoDocument> getAllMovieInfos(Map<MovieInfoFilterCriteria, String> filterCriteria) {
        String year = filterCriteria.getOrDefault(MovieInfoFilterCriteria.year.name(), "0");
        String name = filterCriteria.get(MovieInfoFilterCriteria.name.name());
        if (year != null && Integer.parseInt(year) > 0) {
            return movieInfoService.getAllMoviesByYear(Integer.parseInt(year)).log();
        } else if (StringUtils.hasLength(name)) {
            return movieInfoService.getAllMoviesByName(name).log();
        } else {
            return movieInfoService.getAllMovies().log();
        }
    }

    @Override
    public Mono<ResponseEntity<MovieInfoDocument>> getMovieInfo(String movieInfoId) {
        return movieInfoService.getMovieById(movieInfoId)
                .map(ResponseEntity.ok()::body)
                .switchIfEmpty(Mono.just(ResponseEntity
                        .notFound()
                        .header("X-Reason", "movie info id cannot be found")
                        .build()))
                .log();
    }

    // use ResponseEntity to handle notFound() case
    @Override
    public Mono<ResponseEntity<MovieInfoDocument>> upsertMovieInfo(MovieInfoDocument movieInfoDocument, String movieInfoId) {
        return movieInfoService.upsertMovieInfo(movieInfoDocument, movieInfoId)
                .map(ResponseEntity.ok()::body)
                .switchIfEmpty(Mono.just(ResponseEntity.notFound().build()))
                .log();
    }

    @Override
    public Mono<Void> deleteMovieInfo(String movieInfoId) {
        return movieInfoService.deleteMovieInfo(movieInfoId);
    }

}
