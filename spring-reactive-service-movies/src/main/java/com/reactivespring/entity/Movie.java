package com.reactivespring.entity;

import java.util.List;

public record Movie(MovieInfo movieInfo, List<Review> reviewList) {}
