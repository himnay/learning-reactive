package com.reactivespring.handler;

import com.reactivespring.exception.ReviewDataException;
import com.reactivespring.exception.ReviewNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.reactive.error.ErrorWebExceptionHandler;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * There are 2 responsibilities of this class
 * 1. centralize exception handling throw from controller class.
 * 2. customize exception handling error messages.
 */
@Slf4j
@Component
public class GlobalExceptionHandler implements ErrorWebExceptionHandler {

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        log.error("Exception [ {} ]", ex.getMessage());
        DataBufferFactory dataBufferFactory = exchange.getResponse().bufferFactory();
        DataBuffer dataBuffer = dataBufferFactory.wrap(ex.getMessage().getBytes());

        if (ex instanceof ReviewDataException) {
            exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
        } else if (ex instanceof ReviewNotFoundException) {
            exchange.getResponse().setStatusCode(HttpStatus.NOT_FOUND);
        } else {
            exchange.getResponse().setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return exchange.getResponse().writeWith(Mono.just(dataBuffer));
    }
}
