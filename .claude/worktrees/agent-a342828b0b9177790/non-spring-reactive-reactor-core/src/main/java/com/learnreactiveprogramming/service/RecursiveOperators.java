package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;

import java.util.List;

/**
 * Recursive expansion operators for tree/graph traversal without manual recursion.
 * expand() uses BFS (breadth-first); expandDeep() uses DFS (depth-first).
 * Both apply a function to each emitted item to produce more items.
 */
public class RecursiveOperators {

    /**
     * expand (BFS): processes all items at the current level before going deeper.
     * The result order follows breadth-first discovery (level by level).
     * Tree: 1 → [2,3] → [4,5,6,7]  emits: 1,2,3,4,5,6,7
     */
    public Flux<Integer> expandBreadthFirst() {
        return Flux.just(1)
                .expand(n -> n < 8 ? Flux.just(n * 2, n * 2 + 1) : Flux.empty());
    }

    /**
     * expandDeep (DFS): follows each branch to its end before starting the next.
     * The result order is depth-first (like a recursive pre-order traversal).
     * For the same tree: emits 1,2,4,5,3,6,7 (DFS pre-order)
     */
    public Flux<Integer> expandDeepFirst() {
        return Flux.just(1)
                .expandDeep(n -> n < 8 ? Flux.just(n * 2, n * 2 + 1) : Flux.empty());
    }

    /**
     * Simulates recursive directory traversal using expand (BFS).
     * Real implementation would use Files.list() wrapped in Flux.fromIterable.
     */
    record FileNode(String path, List<String> children) {}

    public Flux<String> directoryTraversal() {
        FileNode root = new FileNode("/root", List.of("/root/a", "/root/b"));
        FileNode a = new FileNode("/root/a", List.of("/root/a/1.txt"));
        FileNode b = new FileNode("/root/b", List.of());

        return Flux.just(root)
                .expand(node -> Flux.fromIterable(node.children())
                        .map(child -> child.equals("/root/a") ? a : b))
                .map(FileNode::path);
    }
}
