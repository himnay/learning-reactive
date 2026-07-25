package com.reactivespring.r2dbc.controller;

import com.reactivespring.r2dbc.entity.Genre;
import com.reactivespring.r2dbc.service.GenreService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/v1/genres")
public class GenreController {

    private final GenreService genreService;

    public GenreController(GenreService genreService) {
        this.genreService = genreService;
    }

    /** Finds all. */
    @GetMapping
    public Flux<Genre> findAll() {
        return genreService.findAll();
    }

    /** Finds by id. */
    @GetMapping("/{id}")
    public Mono<Genre> findById(@PathVariable Long id) {
        return genreService.findById(id);
    }

    /** Searches. */
    @GetMapping("/search")
    public Flux<Genre> search(@RequestParam String name) {
        return genreService.search(name);
    }

    /** Creates. */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Genre> create(@RequestBody @Valid Genre genre) {
        return genreService.create(genre);
    }

    // Bulk insert — accepts a JSON array. collectList() (not a raw Flux<T> return) is required
    // here: WebFlux commits the HTTP response status on the FIRST emitted element of a streamed
    // Flux<T> body, before the rest of the pipeline (including the transactional rollback) has
    // even run — a mid-stream failure would otherwise surface as a truncated 201 instead of a
    // clean error status. Buffering into Mono<List<Genre>> defers the status commit until the
    // whole transactional pipeline has resolved, one way or the other.
    /** Creates batch. */
    @PostMapping("/batch")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<List<Genre>> createBatch(@RequestBody List<@Valid Genre> genres) {
        return genreService.createBatch(genres).collectList();
    }

    /** Updates. */
    @PutMapping("/{id}")
    public Mono<Genre> update(@PathVariable Long id, @RequestBody @Valid Genre genre) {
        return genreService.update(id, genre);
    }

    /** Deletes. */
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> delete(@PathVariable Long id) {
        return genreService.delete(id);
    }
}
