package com.picturecompressor.config;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.reactive.ServerHttpRequest;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Configuration for rate limiting
 */
@Configuration
public class RateLimitConfig {

    public static final int NUM_TOKENS = 1;
    public static final String IP_TEMPLATE = "ip:%s";
    public static final String DEFAULT_IP = "0.0.0.0";
    public static final String ERROR_MESSAGE = "Rate limit exceeded. Try again later.";

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

    public static String getClientIdentifier(ServerHttpRequest request) {
        return Optional.ofNullable(request.getRemoteAddress())
                .map(InetSocketAddress::getHostString)
                .map(RateLimitConfig.IP_TEMPLATE::formatted)
                .orElse(RateLimitConfig.DEFAULT_IP);
    }
} 