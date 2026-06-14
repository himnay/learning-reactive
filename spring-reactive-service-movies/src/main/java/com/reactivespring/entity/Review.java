package com.reactivespring.entity;

public record Review(String reviewId, Long movieInfoId, String comment, Double rating) {}
