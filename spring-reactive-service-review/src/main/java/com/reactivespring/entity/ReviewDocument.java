package com.reactivespring.entity;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document
public record ReviewDocument(
        @Id String reviewId,
        @NotNull(message = "Review.movieInfoId cannot be null") Long movieInfoId,
        String comment,
        @Min(value = 0L, message = "rating.negative : please pass a non-negative value") Double rating
) {}
