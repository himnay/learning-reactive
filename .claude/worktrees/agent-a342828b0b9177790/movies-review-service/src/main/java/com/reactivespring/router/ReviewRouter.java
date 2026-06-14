package com.reactivespring.router;

import com.reactivespring.handler.ReviewHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RequestPredicates.path;
import static org.springframework.web.reactive.function.server.RequestPredicates.queryParam;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@Configuration
@RequiredArgsConstructor
public class ReviewRouter {

    private final ReviewHandler reviewHandler;

    // router + handler
    @Bean
    public RouterFunction<ServerResponse> reviewRouterFunction() {
        return
                route()
                        .nest(path("/v1/reviews"), builder -> { // share the common uri between REST APIs
                            builder
                                    .POST("", request -> reviewHandler.addReview(request))
                                    .GET("", request -> reviewHandler.getReviews(request))
                                    .GET("", queryParam("movieInfoId", i -> true), request -> reviewHandler.getReviews(request))
                                    .GET("/{reviewId}", request -> reviewHandler.getReviews(request))
                                    .GET("/stream", request -> reviewHandler.getReviewsStream(request))
                                    .PUT("/{reviewId}", request -> reviewHandler.upsertReview(request))
                                    .DELETE("/{reviewId}", request -> reviewHandler.deleteReview(request));

                        })
                        .GET("/v1/helloworld", request -> ServerResponse.ok().bodyValue("helloworld"))
                        .build();
    }
}
