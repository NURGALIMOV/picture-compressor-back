package com.picturecompressor.exception;

/**
 * Exception thrown when an invalid compression level is provided
 */
public class InvalidCompressionLevelException extends RuntimeException {
    public InvalidCompressionLevelException(String message) {
        super(message);
    }
} 