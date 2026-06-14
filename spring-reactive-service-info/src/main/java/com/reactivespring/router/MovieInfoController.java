package com.reactivespring.router;

import com.reactivespring.api.MovieInfoApi;
import com.reactivespring.entity.MovieInfoDocument;
import com.reactivespring.outbox.OutboxService;
import com.reactivespring.projection.MovieInfoProjectionRepository;
import com.reactivespring.projection.MovieSummary;
import com.reactivespring.repository.MovieInfoRepository;
import com.reactivespring.service.MovieInfoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Map;

@RestController
public class MovieInfoController implements MovieInfoApi {

    private static final Logger log = LoggerFactory.getLogger(MovieInfoController.class);

    private final Sinks.Many<MovieInfoDocument> movieInfoSinks = Sinks.many().replay().all();

    private final MovieInfoService movieInfoService;
    private final OutboxService outboxService;
    private final MovieInfoProjectionRepository projectionRepository;
    private final MovieInfoRepository movieInfoRepository;

    public MovieInfoController(MovieInfoService movieInfoService,
                               OutboxService outboxService,
                               MovieInfoProjectionRepository projectionRepository,
                               MovieInfoRepository movieInfoRepository) {
        this.movieInfoService = movieInfoService;
        this.outboxService = outboxService;
        this.projectionRepository = projectionRepository;
        this.movieInfoRepository = movieInfoRepository;
    }

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
    public Flux<ServerSentEvent<MovieInfoDocument>> getMovieInfoStream() {
        return movieInfoSinks.asFlux()
                .map(doc -> ServerSentEvent.<MovieInfoDocument>builder()
                        .id(doc.movieInfoId())
                        .event("movie-info")
                        .data(doc)
                        .build());
    }

    @Override
    public Mono<MovieInfoDocument> createMovieInfo(MovieInfoDocument movieInfoDocument) {
        return movieInfoService.createMovieInfo(movieInfoDocument)
                .flatMap(saved -> {
                    log.info("Created movie info: {}", saved.movieInfoId());
                    movieInfoSinks.tryEmitNext(saved);
                    return outboxService.saveEvent(saved.movieInfoId(), "MOVIE_CREATED", saved.name())
                            .thenReturn(saved);
                })
                .log();
    }

    @Override
    public Flux<MovieInfoDocument> getAllMovieInfos(Map<String, String> filterCriteria) {
        String year = filterCriteria.getOrDefault("year", "0");
        String name = filterCriteria.get("name");
        Flux<MovieInfoDocument> result;
        if (year != null && Integer.parseInt(year) > 0) {
            result = movieInfoService.getAllMoviesByYear(Integer.parseInt(year));
        } else if (StringUtils.hasLength(name)) {
            result = movieInfoService.getAllMoviesByName(name);
        } else {
            result = movieInfoService.getAllMovies();
        }
        return result.name("movie.info.get.all").metrics().log();
    }

    @Override
    public Mono<ResponseEntity<MovieInfoDocument>> getMovieInfo(String movieInfoId) {
        return movieInfoService.getMovieById(movieInfoId)
                .map(doc -> ResponseEntity.ok().body(doc))
                .switchIfEmpty(Mono.just(ResponseEntity
                        .notFound()
                        .header("X-Reason", "movie info id cannot be found")
                        .build()))
                .log();
    }

    @Override
    public Mono<ResponseEntity<MovieInfoDocument>> upsertMovieInfo(MovieInfoDocument movieInfoDocument, String movieInfoId) {
        return movieInfoService.upsertMovieInfo(movieInfoDocument, movieInfoId)
                .map(doc -> ResponseEntity.ok().body(doc))
                .switchIfEmpty(Mono.just(ResponseEntity.<MovieInfoDocument>notFound().build()))
                .log();
    }

    @Override
    public Mono<Void> deleteMovieInfo(String movieInfoId) {
        return movieInfoService.deleteMovieInfo(movieInfoId);
    }

    @Override
    public Flux<MovieInfoDocument> getMovieInfoTailable() {
        return movieInfoRepository.findWithTailableCursorBy();
    }

    @Override
    public Flux<MovieInfoDocument> batchCreateMovieInfo(Flux<MovieInfoDocument> documents) {
        return movieInfoService.saveAllReactive(documents);
    }

    @Override
    public Flux<MovieSummary> getAllMovieSummaries() {
        return projectionRepository.findAllSummaries();
    }

    @Override
    public Mono<MovieSummary> getMovieSummary(String movieInfoId) {
        return projectionRepository.findSummaryById(movieInfoId);
    }
}
