package com.learnreactiveprogramming.model;

import java.util.List;

public record FileNode(String path, List<String> children) {}
