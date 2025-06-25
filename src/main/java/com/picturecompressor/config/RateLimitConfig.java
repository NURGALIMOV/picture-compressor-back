package com.picturecompressor.config;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Configuration for rate limiting
 */
@Configuration
public class RateLimitConfig {

    @Value("${rate-limiting.requests-per-minute}")
    private int requestsPerMinute;

    /**
     * Cache to hold rate limit buckets per IP address
     */
    @Bean
    public Map<String, Bucket> buckets() {
        return new ConcurrentHashMap<>();
    }

    /**
     * Creates a new rate limit bucket with the configured requests per minute
     * 
     * @return A new bucket instance
     */
    public Bucket createNewBucket() {
        Bandwidth limit = Bandwidth.builder()
                .capacity(requestsPerMinute)
                .refillGreedy(requestsPerMinute, Duration.ofMinutes(1))
                .build();
        return Bucket.builder().addLimit(limit).build();
    }
} 