package com.reactivespring.handler;

import com.reactivespring.exception.MoviesInfoClientException;
import com.reactivespring.exception.MoviesInfoServerException;
import com.reactivespring.exception.ReviewsClientException;
import com.reactivespring.exception.ReviewsServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MoviesInfoClientException.class)
    public ResponseEntity<String> handleMoviesInfoClientException(MoviesInfoClientException ex) {
        log.error("MoviesInfoClientException: {}", ex.getMessage());
        return ResponseEntity.status(ex.getStatusCode()).body(ex.getMessage());
    }

    @ExceptionHandler(MoviesInfoServerException.class)
    public ResponseEntity<String> handleMoviesInfoServerException(MoviesInfoServerException ex) {
        log.error("MoviesInfoServerException: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ex.getMessage());
    }

    @ExceptionHandler(ReviewsClientException.class)
    public ResponseEntity<String> handleReviewsClientException(ReviewsClientException ex) {
        log.error("ReviewsClientException: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ex.getMessage());
    }

    @ExceptionHandler(ReviewsServerException.class)
    public ResponseEntity<String> handleReviewsServerException(ReviewsServerException ex) {
        log.error("ReviewsServerException: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ex.getMessage());
    }
}
