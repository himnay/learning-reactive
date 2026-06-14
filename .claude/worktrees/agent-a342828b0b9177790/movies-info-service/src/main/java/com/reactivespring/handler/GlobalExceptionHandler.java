package com.reactivespring.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.support.DefaultMessageSourceResolvable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.support.WebExchangeBindException;

import java.util.stream.Collectors;

/**
 * There are 2 responsibilities of this class
 * 1. centralize exception handling throw from controller class.
 * 2. customize exception handling error messages.
 */
@Slf4j
@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(WebExchangeBindException.class)
    public ResponseEntity<String> handleRequestBodyError(WebExchangeBindException webExchangeBindException) {
        log.error("Exception caught [ {} ]", webExchangeBindException.getMessage(), webExchangeBindException);
        var errorList = webExchangeBindException.getBindingResult().getAllErrors().stream().map(DefaultMessageSourceResolvable::getDefaultMessage)
                .sorted()
                .collect(Collectors.joining(","));

        log.error("Error is [ {} ]", errorList);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorList);
    }
}
