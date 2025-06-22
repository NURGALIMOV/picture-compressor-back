package com.example.picturecompressor.exception;

/**
 * Exception thrown when system resources are insufficient to handle the request
 */
public class InsufficientResourcesException extends RuntimeException {
    public InsufficientResourcesException(String message) {
        super(message);
    }
    
    public InsufficientResourcesException(String message, Throwable cause) {
        super(message, cause);
    }
} 