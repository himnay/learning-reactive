package com.reactivespring.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@Data
@Document
@NoArgsConstructor
@AllArgsConstructor
public class ReviewDocument {

    @Id
    private String reviewId;

    @NotNull(message = "Review.movieInfoId cannot be null")
    private Long movieInfoId;

    private String comment;

    @Min(value = 0L, message = "rating.negative : please pass a non-negative value")
    private Double rating;
}
