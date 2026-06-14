package com.reactivespring.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.reactivespring.entity.MovieInfoDocument;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class ReactiveRedisConfig {

    @Bean
    public ReactiveRedisTemplate<String, MovieInfoDocument> movieInfoRedisTemplate(
            ReactiveRedisConnectionFactory factory) {

        // Jackson serializer configured with JavaTimeModule so that LocalDate fields
        // in MovieInfoDocument (a Java record) serialize/deserialize correctly.
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        Jackson2JsonRedisSerializer<MovieInfoDocument> valueSerializer =
                new Jackson2JsonRedisSerializer<>(mapper, MovieInfoDocument.class);

        RedisSerializationContext<String, MovieInfoDocument> context =
                RedisSerializationContext.<String, MovieInfoDocument>newSerializationContext(
                                new StringRedisSerializer())
                        .value(valueSerializer)
                        .build();

        return new ReactiveRedisTemplate<>(factory, context);
    }
}
