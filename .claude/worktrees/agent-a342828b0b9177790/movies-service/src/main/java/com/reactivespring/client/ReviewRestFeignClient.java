package com.reactivespring.client;

import com.reactivespring.entity.Review;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import reactivefeign.spring.config.ReactiveFeignClient;
import reactor.core.publisher.Flux;

@ReactiveFeignClient(value = "movie-review-service", url = "${movies.url.reviews}")
public interface ReviewRestFeignClient {

    @GetMapping("/v1/reviews")
    Flux<Review> getReview(@RequestParam("movieInfoId") String movieInfoId);
}
