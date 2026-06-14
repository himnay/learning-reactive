package com.reactivespring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactivefeign.spring.config.EnableReactiveFeignClients;

@SpringBootApplication
@EnableReactiveFeignClients
public class MoviesServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(MoviesServiceApplication.class, args);
    }

}
