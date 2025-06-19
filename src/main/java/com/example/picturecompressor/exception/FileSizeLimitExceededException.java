package com.example.picturecompressor.exception;

/**
 * Exception thrown when file size exceeds the allowed limit
 */
public class FileSizeLimitExceededException extends RuntimeException {
    public FileSizeLimitExceededException(String message) {
        super(message);
    }
} 