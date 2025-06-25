package com.picturecompressor.exception;

/**
 * Exception thrown when rate limit is exceeded
 */
public class RateLimitExceededException extends RuntimeException {
    public RateLimitExceededException(String message) {
        super(message);
    }
} 