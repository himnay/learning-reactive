package com.reactivespring.websocket;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Component
public class MovieInfoWebSocketHandler implements WebSocketHandler {

    // Multicast sink: each message received from any client is broadcast to all connected clients.
    // onBackpressureBuffer retains messages until subscribers consume them.
    private final Sinks.Many<String> messageSink = Sinks.many().multicast().onBackpressureBuffer();

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        // Receive messages from the client, broadcast each one via the shared sink
        Mono<Void> receive = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(messageSink::tryEmitNext)
                .then();

        // Send all broadcast messages (including from other sessions) back to this client
        Mono<Void> send = session.send(
                messageSink.asFlux().map(session::textMessage));

        // zip: both receive and send must run concurrently; cancelling either cancels both.
        return Mono.zip(receive, send).then();
    }
}
