package com.reactivespring.handler;

import com.reactivespring.exception.MoviesInfoClientException;
import com.reactivespring.exception.MoviesInfoServerException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@Slf4j
@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MoviesInfoClientException.class)
    public ResponseEntity<String> handleClientException(MoviesInfoClientException exception) {
        log.error("MoviesClientException : ", exception);
        return ResponseEntity
                .status(exception.getStatusCode())
                .body(exception.getMessage());
    }

    @ExceptionHandler(MoviesInfoServerException.class)
    public ResponseEntity<String> handleClientException(MoviesInfoServerException exception) {
        log.error("MoviesClientException : ", exception);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(exception.getMessage());
    }

}
