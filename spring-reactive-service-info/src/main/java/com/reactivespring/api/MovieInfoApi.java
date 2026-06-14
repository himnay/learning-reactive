package com.reactivespring.api;

import com.reactivespring.entity.MovieInfoDocument;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

@RequestMapping("/v1")
public interface MovieInfoApi {

    @GetMapping("/flux")
    Flux<Integer> nonBlockingFlux();

    @GetMapping("/mono")
    Mono<String> nonBlockingMono();

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<Long> nonBlockingFluxStream();

    // SSE stream using proper ServerSentEvent<T> wrapper so clients receive
    // event id, event type, and data fields in the SSE protocol format.
    @GetMapping(value = "/movieInfo/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<ServerSentEvent<MovieInfoDocument>> getMovieInfoStream();

    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping(value = "/movieInfo", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    Mono<MovieInfoDocument> createMovieInfo(@RequestBody @Valid MovieInfoDocument movieInfoDocument);

    @GetMapping(value = "/movieInfos", produces = MediaType.APPLICATION_JSON_VALUE)
    Flux<MovieInfoDocument> getAllMovieInfos(@RequestParam Map<String, String> filterCriteria);

    @GetMapping(value = "/movieInfo/{movieInfoId}", produces = MediaType.APPLICATION_JSON_VALUE)
    Mono<ResponseEntity<MovieInfoDocument>> getMovieInfo(@PathVariable String movieInfoId);

    @PutMapping(value = "/movieInfo/{movieInfoId}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    Mono<ResponseEntity<MovieInfoDocument>> upsertMovieInfo(@RequestBody @Valid MovieInfoDocument movieInfoDocument, @PathVariable String movieInfoId);

    @ResponseStatus(HttpStatus.NO_CONTENT)
    @DeleteMapping(value = "/movieInfo/{movieInfoId}")
    Mono<Void> deleteMovieInfo(@PathVariable String movieInfoId);

    // Tailable cursor endpoint — streams new documents in real-time from a capped collection
    @GetMapping(value = "/movieInfo/tailable", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<MovieInfoDocument> getMovieInfoTailable();

    // Bulk insert endpoint — accepts a streaming request body for memory-efficient large inserts
    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping(value = "/movieInfo/batch", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    Flux<MovieInfoDocument> batchCreateMovieInfo(@RequestBody Flux<MovieInfoDocument> documents);

    // CQRS projections — lightweight read model with only summary fields
    @GetMapping(value = "/movieInfo/summary", produces = MediaType.APPLICATION_JSON_VALUE)
    Flux<com.reactivespring.projection.MovieSummary> getAllMovieSummaries();

    @GetMapping(value = "/movieInfo/{movieInfoId}/summary", produces = MediaType.APPLICATION_JSON_VALUE)
    Mono<com.reactivespring.projection.MovieSummary> getMovieSummary(@PathVariable String movieInfoId);
}
