package com.reactivespring.outbox;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Transactional outbox pattern: an outbox event is persisted in the same database
 * write as the domain aggregate.  A separate relay process then reads PENDING events
 * and publishes them to the message broker, marking them SENT on success.
 * This avoids dual-write problems between the DB and the broker.
 */
@Document(collection = "outbox_events")
public record OutboxEvent(
        @Id String eventId,
        String aggregateId,
        String eventType,
        String payload,
        String status,   // PENDING | SENT
        Instant createdAt
) {
    public static final String PENDING = "PENDING";
    public static final String SENT = "SENT";
}
