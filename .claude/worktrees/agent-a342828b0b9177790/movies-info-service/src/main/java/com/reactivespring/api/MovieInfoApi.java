package com.reactivespring.api;

import com.reactivespring.entity.MovieInfoDocument;
import com.reactivespring.enums.MovieInfoFilterCriteria;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.validation.Valid;
import java.util.Map;

@RequestMapping("/v1")
public interface MovieInfoApi {

    @GetMapping("/flux")
    Flux<Integer> nonBlockingFlux();

    @GetMapping("/mono")
    Mono<String> nonBlockingMono();

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<Long> nonBlockingFluxStream();

    // recommended media type is APPLICATION_NDJSON_VALUE but switch to APPLICATION_NDJSON_VALUE wont work in chrome browsers
    @GetMapping(value = "/movieInfo/stream", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
    Flux<MovieInfoDocument> getMovieInfoStream();

    // @Valid is mandatory to bootstrap the validation framework. without this none of the validation annotation works.
    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping(value = "/movieInfo", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    Mono<MovieInfoDocument> createMovieInfo(@RequestBody @Valid MovieInfoDocument movieInfoDocument);

    @GetMapping(value = "/movieInfos", produces = MediaType.APPLICATION_JSON_VALUE)
    Flux<MovieInfoDocument> getAllMovieInfos(@RequestParam Map<MovieInfoFilterCriteria, String> filterCriteria);

    @GetMapping(value = "/movieInfo/{movieInfoId}", produces = MediaType.APPLICATION_JSON_VALUE)
    Mono<ResponseEntity<MovieInfoDocument>> getMovieInfo(@PathVariable String movieInfoId);

    @PutMapping(value = "/movieInfo/{movieInfoId}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    Mono<ResponseEntity<MovieInfoDocument>> upsertMovieInfo(@RequestBody @Valid MovieInfoDocument movieInfoDocument, @PathVariable String movieInfoId);

    @ResponseStatus(HttpStatus.NO_CONTENT)
    @DeleteMapping(value = "/movieInfo/{movieInfoId}")
    Mono<Void> deleteMovieInfo(@PathVariable String movieInfoId);
}
