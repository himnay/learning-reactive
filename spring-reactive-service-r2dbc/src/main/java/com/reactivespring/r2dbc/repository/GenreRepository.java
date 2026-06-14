package com.reactivespring.r2dbc.repository;

import com.reactivespring.r2dbc.entity.Genre;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface GenreRepository extends ReactiveCrudRepository<Genre, Long> {

    Flux<Genre> findByNameContainingIgnoreCase(String name);

    Mono<Genre> findByName(String name);

    // R2DBC supports @Query for custom SQL — fully non-blocking via PostgreSQL R2DBC driver
    @Query("SELECT * FROM genres WHERE created_at > NOW() - INTERVAL '1 day' ORDER BY created_at DESC")
    Flux<Genre> findRecentGenres();
}
