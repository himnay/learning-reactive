package com.reactivespring.router;

import com.reactivespring.handler.ReviewHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RequestPredicates.path;
import static org.springframework.web.reactive.function.server.RequestPredicates.queryParam;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@Configuration
public class ReviewRouter {

    private final ReviewHandler reviewHandler;

    public ReviewRouter(ReviewHandler reviewHandler) {
        this.reviewHandler = reviewHandler;
    }

    @Bean
    public RouterFunction<ServerResponse> reviewRouterFunction() {
        return route()
                .nest(path("/v1/reviews"), builder -> builder
                        .POST("", reviewHandler::addReview)
                        .GET("", reviewHandler::getReviews)
                        .GET("", queryParam("movieInfoId", i -> true), reviewHandler::getReviews)
                        .GET("/{reviewId}", reviewHandler::getReviews)
                        .GET("/stream", reviewHandler::getReviewsStream)
                        .PUT("/{reviewId}", reviewHandler::upsertReview)
                        .DELETE("/{reviewId}", reviewHandler::deleteReview))
                .GET("/v1/helloworld", req -> ServerResponse.ok().bodyValue("helloworld"))
                .build();
    }
}
