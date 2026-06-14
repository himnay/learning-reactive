package com.reactivespring.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
public class OutboxService {

    private static final Logger log = LoggerFactory.getLogger(OutboxService.class);

    private final OutboxRepository outboxRepository;

    public OutboxService(OutboxRepository outboxRepository) {
        this.outboxRepository = outboxRepository;
    }

    /**
     * Persist an outbox event as PENDING.
     * Called in the same reactive chain as the domain aggregate save so that both
     * writes are attempted atomically (best-effort; true atomicity requires a MongoDB
     * multi-document transaction or change streams).
     */
    public Mono<OutboxEvent> saveEvent(String aggregateId, String eventType, String payload) {
        OutboxEvent event = new OutboxEvent(
                UUID.randomUUID().toString(),
                aggregateId,
                eventType,
                payload,
                OutboxEvent.PENDING,
                Instant.now());
        return outboxRepository.save(event);
    }

    /**
     * Relay: runs on a fixed schedule, finds PENDING events, "publishes" them (here: logs),
     * and marks them SENT.  In production, replace the log statement with an actual
     * message broker call (Kafka/RabbitMQ) before marking SENT.
     */
    @Scheduled(fixedDelay = 5000)
    public void relayPendingEvents() {
        outboxRepository.findByStatus(OutboxEvent.PENDING)
                .flatMap(event -> {
                    log.info("Relaying outbox event: type={} aggregateId={}", event.eventType(), event.aggregateId());
                    OutboxEvent sent = new OutboxEvent(
                            event.eventId(),
                            event.aggregateId(),
                            event.eventType(),
                            event.payload(),
                            OutboxEvent.SENT,
                            event.createdAt());
                    return outboxRepository.save(sent);
                })
                .subscribe(
                        e -> log.debug("Marked event {} as SENT", e.eventId()),
                        err -> log.error("Outbox relay error", err));
    }
}
