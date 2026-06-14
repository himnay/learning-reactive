package com.reactivespring.projection;

/**
 * CQRS read-model projection: only the fields needed for list views.
 * Avoids fetching cast lists and release dates for endpoints that don't need them.
 */
public record MovieSummary(
        String movieInfoId,
        String name,
        Integer year
) {}
