package com.reactivespring.health;

import com.reactivespring.repository.MovieInfoRepository;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class MovieInfoHealthIndicator implements ReactiveHealthIndicator {

    private final MovieInfoRepository repository;

    public MovieInfoHealthIndicator(MovieInfoRepository repository) {
        this.repository = repository;
    }

    @Override
    public Mono<Health> health() {
        return repository.count()
                .map(count -> Health.up()
                        .withDetail("documentCount", count)
                        .withDetail("service", "movie-info")
                        .build())
                // MongoDB unreachable causes count() to error; surface that as DOWN
                .onErrorReturn(Health.down()
                        .withDetail("reason", "MongoDB unreachable")
                        .build());
    }
}
