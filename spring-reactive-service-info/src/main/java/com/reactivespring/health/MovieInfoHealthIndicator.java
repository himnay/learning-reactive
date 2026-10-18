package com.reactivespring.health;

import com.reactivespring.repository.MovieInfoRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MovieInfoHealthIndicator implements ReactiveHealthIndicator {

    private final MovieInfoRepository repository;

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
