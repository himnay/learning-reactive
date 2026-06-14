package com.reactivespring.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import java.time.LocalDate;
import java.util.List;

@Data
@Document
@NoArgsConstructor
@AllArgsConstructor
public class MovieInfoDocument {

    @Id
    private String movieInfoId;

    @NotBlank(message = "MovieInfo.name cannot be empty")
    private String name;

    @NotNull
    @Positive(message = "MovieInfo.year must be a positive no")
    private Integer year;

    @NotNull(message = "MovieInfo.cast cannot be empty")
    private List<@NotBlank(message = "MovieInfo.cast cannot be empty") String> cast;

    private LocalDate releaseDate;
}
