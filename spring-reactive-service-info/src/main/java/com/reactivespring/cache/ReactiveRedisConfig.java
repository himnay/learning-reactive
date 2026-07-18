package com.reactivespring.cache;

import com.reactivespring.entity.MovieInfoDocument;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class ReactiveRedisConfig {

    /** Defines the movie info redis template bean. */
    @Bean
    public ReactiveRedisTemplate<String, MovieInfoDocument> movieInfoRedisTemplate(
            ReactiveRedisConnectionFactory factory) {

        // Jackson 3 serializer — java.time support is built in and dates serialize
        // as ISO-8601 by default, so LocalDate fields in MovieInfoDocument just work.
        JacksonJsonRedisSerializer<MovieInfoDocument> valueSerializer =
                new JacksonJsonRedisSerializer<>(MovieInfoDocument.class);

        RedisSerializationContext<String, MovieInfoDocument> context =
                RedisSerializationContext.<String, MovieInfoDocument>newSerializationContext(
                                new StringRedisSerializer())
                        .value(valueSerializer)
                        .build();

        return new ReactiveRedisTemplate<>(factory, context);
    }
}
