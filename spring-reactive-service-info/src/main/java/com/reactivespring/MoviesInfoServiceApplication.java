package com.reactivespring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
class MoviesInfoServiceApplication {

    /** Application entry point. */
    public static void main(String[] args) {
        SpringApplication.run(MoviesInfoServiceApplication.class, args);
    }
}
