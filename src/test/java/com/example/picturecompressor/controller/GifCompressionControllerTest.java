package com.example.picturecompressor.controller;

import com.example.picturecompressor.config.RateLimitConfig;
import com.example.picturecompressor.service.GifCompressionService;
import io.github.bucket4j.Bucket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for GifCompressionController
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GifCompressionControllerTest {

    @Mock
    private GifCompressionService compressionService;

    @Mock
    private Map<String, Bucket> buckets;
    
    @Mock
    private RateLimitConfig rateLimitConfig;
    
    @InjectMocks
    private GifCompressionController controller;

    private FilePart mockFilePart;
    private ServerHttpRequest mockRequest;
    private final byte[] compressedData = "mock compressed data".getBytes();

    @BeforeEach
    void setUp() {
        // Set up mock bucket
        Bucket mockBucket = mock(Bucket.class);
        when(mockBucket.tryConsume(1)).thenReturn(true);
        
        // Set up rate limit config
        when(rateLimitConfig.createNewBucket()).thenReturn(mockBucket);
        when(buckets.computeIfAbsent(any(), any())).thenReturn(mockBucket);
        
        // Set up mock file part
        mockFilePart = mock(FilePart.class);
        when(mockFilePart.filename()).thenReturn("test.gif");
        
        // Setup mock request with IP address
        mockRequest = MockServerHttpRequest.get("/")
                .remoteAddress(new java.net.InetSocketAddress("127.0.0.1", 12345))
                .build();
    }

    @Test
    void compressGifTest() {
        // Mock the service
        when(compressionService.compressGif(any(FilePart.class), anyFloat()))
                .thenReturn(Mono.just(compressedData));

        // Call controller method directly
        var result = controller.compressGif(mockFilePart, 0.5f, mockRequest);
        Assertions.assertNotNull(result);
        // Verify response
        StepVerifier.create(result)
                .expectNextMatches(
                        response -> (response.getStatusCode() == HttpStatus.OK) && (response.getBody() == compressedData)
                )
                .verifyComplete();
    }

    @Test
    void compressBatchTest() {
        // Mock the service
        when(compressionService.compressGifBatch(any(Flux.class), anyFloat()))
                .thenReturn(Mono.just(compressedData));

        // Call controller method directly
        var result = controller.compressBatch(Flux.just(mockFilePart), 0.5f, mockRequest);
        
        // Verify response
        StepVerifier.create(result)
                .expectNextMatches(
                        response -> (response.getStatusCode() == HttpStatus.OK) && (response.getBody() == compressedData)
                )
                .verifyComplete();
    }
} 