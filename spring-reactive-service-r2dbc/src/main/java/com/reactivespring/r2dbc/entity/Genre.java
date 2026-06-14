package com.reactivespring.r2dbc.entity;

import jakarta.validation.constraints.NotBlank;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

@Table("genres")
public record Genre(
        @Id Long id,
        @NotBlank(message = "Genre.name cannot be blank") String name,
        String description,
        @CreatedDate LocalDateTime createdAt
) {
    // Compact constructor for creating new instances without an ID (before insert)
    public static Genre of(String name, String description) {
        return new Genre(null, name, description, null);
    }
}
