package com.picturecompressor.controller;

import com.picturecompressor.config.RateLimitConfig;
import com.picturecompressor.exception.RateLimitExceededException;
import com.picturecompressor.service.GifCompressionService;
import io.github.bucket4j.Bucket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Controller for GIF compression API endpoints using fully non-blocking patterns
 */
@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class GifCompressionController {

    private final GifCompressionService compressionService;
    private final Map<String, Bucket> buckets;
    private final RateLimitConfig rateLimitConfig;

    /**
     * Endpoint for compressing a single GIF file
     *
     * @param file The GIF file to compress
     * @param compressionLevel The compression level (0-1)
     * @param request The HTTP request (for client IP extraction)
     * @return The compressed GIF file
     */
    @PostMapping(value = "/compress", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<ResponseEntity<byte[]>> compressGif(
            @RequestPart("file") FilePart file,
            @RequestParam("compressionLevel") float compressionLevel,
            ServerHttpRequest request
    ) {
        return Mono.fromCallable(() -> RateLimitConfig.getClientIdentifier(request))
                .map(this::getRateLimitBucket)
                .map(bucket -> bucket.tryConsume(RateLimitConfig.NUM_TOKENS))
                .flatMap(tryConsume -> tryConsume ?
                        compressionService.compressGif(file, compressionLevel).map(compressedBytes -> createGifResponse(file.filename(), compressedBytes)) :
                        Mono.error(new RateLimitExceededException(RateLimitConfig.ERROR_MESSAGE))
                );
    }

    /**
     * Endpoint for batch compression of multiple GIF files
     *
     * @param files The GIF files to compress
     * @param compressionLevel The compression level (0-1)
     * @param request The HTTP request (for client IP extraction)
     * @return A ZIP archive containing all compressed GIF files
     */
    @PostMapping(value = "/compress/batch", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<ResponseEntity<byte[]>> compressBatch(
            @RequestPart("files") Flux<FilePart> files,
            @RequestParam("compressionLevel") float compressionLevel,
            ServerHttpRequest request
    ) {
        return Mono.fromCallable(() -> RateLimitConfig.getClientIdentifier(request))
                .map(this::getRateLimitBucket)
                .map(bucket -> bucket.tryConsume(RateLimitConfig.NUM_TOKENS))
                .flatMap(tryConsume -> tryConsume ?
                        compressionService.compressGifBatch(files, compressionLevel).map(this::createZipResponse) :
                        Mono.error(new RateLimitExceededException(RateLimitConfig.ERROR_MESSAGE))
                );
    }

    /**
     * Gets or creates a rate limit bucket for the specified client identifier
     *
     * @param clientIdentifier The client identifier
     * @return The rate limit bucket for the client
     */
    private Bucket getRateLimitBucket(String clientIdentifier) {
        log.debug("Getting rate limit bucket for client: {}", clientIdentifier);
        return buckets.computeIfAbsent(clientIdentifier, id -> rateLimitConfig.createNewBucket());
    }

    /**
     * Creates a response with the compressed GIF file
     *
     * @param filename Original filename
     * @param compressedBytes Compressed GIF data
     * @return HTTP response with the compressed file
     */
    private ResponseEntity<byte[]> createGifResponse(String filename, byte[] compressedBytes) {
        return ResponseEntity.ok()
                .contentType(MediaType.IMAGE_GIF)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"compressed_" + filename + "\"")
                .body(compressedBytes);
    }

    /**
     * Creates a response with the ZIP archive
     *
     * @param zipBytes ZIP archive data
     * @return HTTP response with the ZIP archive
     */
    private ResponseEntity<byte[]> createZipResponse(byte[] zipBytes) {
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"compressed_gifs.zip\"")
                .body(zipBytes);
    }
} 