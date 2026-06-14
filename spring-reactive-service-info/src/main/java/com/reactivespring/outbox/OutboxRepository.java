package com.reactivespring.outbox;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface OutboxRepository extends ReactiveMongoRepository<OutboxEvent, String> {

    Flux<OutboxEvent> findByStatus(String status);
}
