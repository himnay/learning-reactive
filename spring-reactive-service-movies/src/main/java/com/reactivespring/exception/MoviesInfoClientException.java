package com.reactivespring.exception;

public class MoviesInfoClientException extends RuntimeException {

    private final int statusCode;

    public MoviesInfoClientException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
