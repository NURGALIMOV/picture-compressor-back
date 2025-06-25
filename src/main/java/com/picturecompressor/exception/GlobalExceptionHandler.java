package com.picturecompressor.exception;

import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ServerWebExchange;

import java.time.LocalDateTime;

/**
 * Global exception handler for handling all exceptions in the application
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(InvalidFileTypeException.class)
    public ResponseEntity<ApiError> handleInvalidFileTypeException(InvalidFileTypeException ex, ServerWebExchange exchange) {
        log.error("Invalid file type: {}", ex.getMessage());
        return createErrorResponse(ex, exchange, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(InvalidCompressionLevelException.class)
    public ResponseEntity<ApiError> handleInvalidCompressionLevelException(InvalidCompressionLevelException ex, ServerWebExchange exchange) {
        log.error("Invalid compression level: {}", ex.getMessage());
        return createErrorResponse(ex, exchange, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(FileSizeLimitExceededException.class)
    public ResponseEntity<ApiError> handleFileSizeLimitExceededException(FileSizeLimitExceededException ex, ServerWebExchange exchange) {
        log.error("File size limit exceeded: {}", ex.getMessage());
        return createErrorResponse(ex, exchange, HttpStatus.PAYLOAD_TOO_LARGE);
    }

    @ExceptionHandler(RateLimitExceededException.class)
    public ResponseEntity<ApiError> handleRateLimitExceededException(RateLimitExceededException ex, ServerWebExchange exchange) {
        log.error("Rate limit exceeded: {}", ex.getMessage());
        return createErrorResponse(ex, exchange, HttpStatus.TOO_MANY_REQUESTS);
    }
    
    @ExceptionHandler(InsufficientResourcesException.class)
    public ResponseEntity<ApiError> handleInsufficientResourcesException(InsufficientResourcesException ex, ServerWebExchange exchange) {
        log.error("Insufficient resources: {}", ex.getMessage());
        return createErrorResponse(ex, exchange, HttpStatus.SERVICE_UNAVAILABLE);
    }
    
    @ExceptionHandler(CallNotPermittedException.class)
    public ResponseEntity<ApiError> handleCircuitBreakerException(CallNotPermittedException ex, ServerWebExchange exchange) {
        log.error("Circuit breaker open: {}", ex.getMessage());
        return createErrorResponse(
            new InsufficientResourcesException("Service temporarily unavailable due to high load. Please try again later."),
            exchange, 
            HttpStatus.SERVICE_UNAVAILABLE
        );
    }
    
    @ExceptionHandler(BulkheadFullException.class)
    public ResponseEntity<ApiError> handleBulkheadFullException(BulkheadFullException ex, ServerWebExchange exchange) {
        log.error("Bulkhead full: {}", ex.getMessage());
        return createErrorResponse(
            new InsufficientResourcesException("Service is processing too many requests. Please try again later."),
            exchange, 
            HttpStatus.TOO_MANY_REQUESTS
        );
    }
    
    @ExceptionHandler(RequestNotPermitted.class)
    public ResponseEntity<ApiError> handleRequestNotPermittedException(RequestNotPermitted ex, ServerWebExchange exchange) {
        log.error("Request not permitted: {}", ex.getMessage());
        return createErrorResponse(
            new RateLimitExceededException("Too many requests. Please try again later."),
            exchange, 
            HttpStatus.TOO_MANY_REQUESTS
        );
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleGenericException(Exception ex, ServerWebExchange exchange) {
        log.error("Generic exception: {}", ex.getMessage());
        return createErrorResponse(ex, exchange, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private ResponseEntity<ApiError> createErrorResponse(Exception ex, ServerWebExchange exchange, HttpStatus status) {
        ApiError error = ApiError.builder()
                .timestamp(LocalDateTime.now())
                .status(status.value())
                .error(status.getReasonPhrase())
                .message(ex.getMessage())
                .path(exchange.getRequest().getPath().value())
                .build();
        return ResponseEntity.status(status).body(error);
    }
} 