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

    @GetMapping
    public Flux<Genre> findAll() {
        return genreService.findAll();
    }

    @GetMapping("/{id}")
    public Mono<Genre> findById(@PathVariable Long id) {
        return genreService.findById(id);
    }

    @GetMapping("/search")
    public Flux<Genre> search(@RequestParam String name) {
        return genreService.search(name);
    }

    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping
    public Mono<Genre> create(@RequestBody @Valid Genre genre) {
        return genreService.create(genre);
    }

    // Bulk insert — accepts a JSON array; internally uses Flux<T> for streaming insert
    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping("/batch")
    public Flux<Genre> createBatch(@RequestBody List<@Valid Genre> genres) {
        return genreService.createBatch(genres);
    }

    @PutMapping("/{id}")
    public Mono<Genre> update(@PathVariable Long id, @RequestBody @Valid Genre genre) {
        return genreService.update(id, genre);
    }

    @ResponseStatus(HttpStatus.NO_CONTENT)
    @DeleteMapping("/{id}")
    public Mono<Void> delete(@PathVariable Long id) {
        return genreService.delete(id);
    }
}
