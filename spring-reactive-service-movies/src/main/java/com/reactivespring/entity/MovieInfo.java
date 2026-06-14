package com.reactivespring.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.LocalDate;
import java.util.List;

public record MovieInfo(
        String movieInfoId,

        @NotBlank(message = "movieInfo.name must be present")
        String name,

        @NotNull
        @Positive(message = "movieInfo.year must be a positive value")
        Integer year,

        @NotNull
        List<@NotBlank(message = "movieInfo.cast must be present") String> cast,

        @JsonProperty("release_date")
        LocalDate releaseDate
) {}
