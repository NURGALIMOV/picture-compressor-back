package com.picturecompressor.config;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.reactor.ratelimiter.operator.RateLimiterOperator;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for Resilience4j patterns to handle resource constraints
 */
@Configuration
@RequiredArgsConstructor
public class ResilienceConfig {

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final BulkheadRegistry bulkheadRegistry;
    private final RateLimiterRegistry rateLimiterRegistry;

    private static final String COMPRESSION_SERVICE = "compressionService";

    /**
     * Creates a circuit breaker for the compression service
     */
    @Bean
    public CircuitBreaker compressionServiceCircuitBreaker() {
        return circuitBreakerRegistry.circuitBreaker(COMPRESSION_SERVICE);
    }

    /**
     * Creates a bulkhead for the compression service
     */
    @Bean
    public Bulkhead compressionServiceBulkhead() {
        return bulkheadRegistry.bulkhead(COMPRESSION_SERVICE);
    }
    
    /**
     * Creates a rate limiter for the compression service
     */
    @Bean
    public RateLimiter compressionServiceRateLimiter() {
        return rateLimiterRegistry.rateLimiter(COMPRESSION_SERVICE);
    }

    /**
     * Creates a circuit breaker operator for reactive streams
     */
    @Bean
    public CircuitBreakerOperator<byte[]> compressionCircuitBreakerOperator() {
        return CircuitBreakerOperator.of(compressionServiceCircuitBreaker());
    }
    
    /**
     * Creates a bulkhead operator for reactive streams
     */
    @Bean
    public BulkheadOperator<byte[]> compressionBulkheadOperator() {
        return BulkheadOperator.of(compressionServiceBulkhead());
    }
    
    /**
     * Creates a rate limiter operator for reactive streams
     */
    @Bean
    public RateLimiterOperator<byte[]> compressionRateLimiterOperator() {
        return RateLimiterOperator.of(compressionServiceRateLimiter());
    }
} 