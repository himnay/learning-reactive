package com.reactivespring.gateway.filter;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * Helper for generating and validating HMAC-SHA256 JWTs using jjwt 0.12.x API.
 * The secret must be at least 256 bits (32 bytes) for HS256.
 */
public final class TokenUtil {

    private TokenUtil() {}

    public static SecretKey signingKey(String secret) {
        // Keys.hmacShaKeyFor requires at least 32 bytes for HS256
        return Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Generate a signed JWT with subject=userId, valid for ttlMillis milliseconds.
     */
    public static String generateToken(String userId, String secret, long ttlMillis) {
        return Jwts.builder()
                .subject(userId)
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + ttlMillis))
                .signWith(signingKey(secret))
                .compact();
    }

    /**
     * Parse and validate the token, returning its Claims.
     * Throws JwtException (subclass of RuntimeException) if invalid or expired.
     */
    public static Claims validateToken(String token, String secret) throws JwtException {
        return Jwts.parser()
                .verifyWith(signingKey(secret))
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }
}
