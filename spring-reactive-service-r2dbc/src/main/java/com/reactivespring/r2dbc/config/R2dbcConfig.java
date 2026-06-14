package com.reactivespring.r2dbc.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.config.EnableR2dbcAuditing;

// Enables @CreatedDate / @LastModifiedDate population on R2DBC entities
@Configuration
@EnableR2dbcAuditing
public class R2dbcConfig {
}
