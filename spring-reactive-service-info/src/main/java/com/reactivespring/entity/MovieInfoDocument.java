package com.reactivespring.entity;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDate;
import java.util.List;

@Document
public record MovieInfoDocument(
        @Id String movieInfoId,
        @NotBlank(message = "MovieInfo.name cannot be empty") String name,
        @NotNull @Positive(message = "MovieInfo.year must be a positive no") Integer year,
        @NotNull(message = "MovieInfo.cast cannot be empty") List<@NotBlank(message = "MovieInfo.cast cannot be empty") String> cast,
        LocalDate releaseDate
) {}
