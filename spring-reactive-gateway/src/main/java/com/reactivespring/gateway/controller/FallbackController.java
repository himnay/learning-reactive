package com.reactivespring.gateway.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Circuit-breaker fallback endpoints returned by the gateway when a downstream
 * service is unavailable.  Each endpoint returns a 503 with a human-readable
 * message so callers get a structured error instead of a raw connection error.
 */
/*
 * The circuit breaker forwards with the original method, so a POST/PUT/DELETE fallback must
 * not hit a GET-only mapping (405): each fallback answers every method.
 */
@RestController
@RequestMapping("/fallback")
public class FallbackController {

    private static final Logger log = LoggerFactory.getLogger(FallbackController.class);

    /** Returns the movie info fallback. */
    @RequestMapping("/movieInfo")
    public Mono<ResponseEntity<String>> movieInfoFallback() {
        log.warn("Circuit breaker open — Movie Info Service unavailable");
        return Mono.just(ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .body("Movie Info Service is temporarily unavailable. Please try again later."));
    }

    /** Returns the reviews fallback. */
    @RequestMapping("/reviews")
    public Mono<ResponseEntity<String>> reviewsFallback() {
        log.warn("Circuit breaker open — Reviews Service unavailable");
        return Mono.just(ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .body("Reviews Service is temporarily unavailable. Please try again later."));
    }

    /** Returns the movies fallback. */
    @RequestMapping("/movies")
    public Mono<ResponseEntity<String>> moviesFallback() {
        log.warn("Circuit breaker open — Movies Service unavailable");
        return Mono.just(ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .body("Movies Service is temporarily unavailable. Please try again later."));
    }
}
