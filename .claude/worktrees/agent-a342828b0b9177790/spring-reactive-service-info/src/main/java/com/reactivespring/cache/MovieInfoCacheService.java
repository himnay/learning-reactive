package com.reactivespring.cache;

import com.reactivespring.entity.MovieInfoDocument;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
public class MovieInfoCacheService {

    private final ReactiveRedisTemplate<String, MovieInfoDocument> redisTemplate;
    private static final Duration TTL = Duration.ofMinutes(10);
    private static final String KEY_PREFIX = "movie:info:";

    public MovieInfoCacheService(ReactiveRedisTemplate<String, MovieInfoDocument> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Cache-aside pattern:
     * 1. Try to GET from Redis.
     * 2. On cache miss (empty), call the loader (typically DB), then SET in Redis with TTL.
     * 3. On cache hit, return the cached value directly without touching the DB.
     */
    public Mono<MovieInfoDocument> getOrLoad(String id, Mono<MovieInfoDocument> loader) {
        String key = KEY_PREFIX + id;
        return redisTemplate.opsForValue().get(key)
                .switchIfEmpty(loader
                        .flatMap(doc -> redisTemplate.opsForValue()
                                .set(key, doc, TTL)
                                .thenReturn(doc)));
    }

    /**
     * Remove the cached entry for this id so the next getOrLoad will reload from DB.
     */
    public Mono<Boolean> evict(String id) {
        return redisTemplate.opsForValue().delete(KEY_PREFIX + id);
    }

    /**
     * Retrieve all cached movie info entries.
     * NOTE: KEYS pattern scanning is O(N) and should not be used in production against
     * large keyspaces.  This is a demo-only implementation.
     */
    public Flux<MovieInfoDocument> getAll() {
        return redisTemplate.keys(KEY_PREFIX + "*")
                .flatMap(key -> redisTemplate.opsForValue().get(key));
    }
}
