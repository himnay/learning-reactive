package com.reactivespring.r2dbc.service;

import com.reactivespring.r2dbc.entity.Genre;
import com.reactivespring.r2dbc.repository.GenreRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
public class GenreService {

    private final GenreRepository repository;
    private final TransactionalOperator txOperator;

    public GenreService(GenreRepository repository, TransactionalOperator txOperator) {
        this.repository = repository;
        this.txOperator = txOperator;
    }

    public Flux<Genre> findAll() {
        return repository.findAll();
    }

    public Mono<Genre> findById(Long id) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Genre not found: " + id)));
    }

    public Mono<Genre> findByName(String name) {
        return repository.findByName(name)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Genre not found: " + name)));
    }

    public Flux<Genre> search(String name) {
        return repository.findByNameContainingIgnoreCase(name);
    }

    public Mono<Genre> create(Genre genre) {
        // TransactionalOperator.transactional(mono) wraps the reactive pipeline in a
        // database transaction. If the Mono emits an error the transaction rolls back.
        // This is the reactive equivalent of @Transactional on a method.
        return txOperator.transactional(repository.save(genre));
    }

    public Mono<Genre> update(Long id, Genre incoming) {
        return txOperator.transactional(
                repository.findById(id)
                        .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND,
                                "Genre not found: " + id)))
                        .flatMap(existing -> repository.save(
                                new Genre(existing.id(), incoming.name(), incoming.description(), existing.createdAt())))
        );
    }

    public Mono<Void> delete(Long id) {
        return txOperator.transactional(
                repository.findById(id)
                        .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND,
                                "Genre not found: " + id)))
                        .flatMap(g -> repository.deleteById(g.id()))
        );
    }

    // Reactive bulk insert — saveAll accepts Flux<T> so the source can be a streaming HTTP body.
    // All inserts run in a single transaction; any failure rolls back all rows.
    public Flux<Genre> createBatch(List<Genre> genres) {
        return txOperator.transactional(
                repository.saveAll(Flux.fromIterable(genres))
        );
    }

    // Demonstrates TransactionalOperator with deliberate rollback:
    // saves two genres atomically; if the second one fails (e.g. duplicate name)
    // the first insert is also rolled back.
    public Flux<Genre> transactionalPairExample(Genre first, Genre second) {
        Flux<Genre> pipeline = repository.save(first)
                .thenMany(repository.save(second).flux());
        return txOperator.transactional(pipeline);
    }
}
