package com.picturecompressor.exception;

/**
 * Exception thrown when an invalid file type is provided
 */
public class InvalidFileTypeException extends RuntimeException {
    public InvalidFileTypeException(String message) {
        super(message);
    }
} 