package com.example.picturecompressor.integration;

import com.example.picturecompressor.PictureCompressorApplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;

/**
 * Integration tests for GIF compression functionality
 * Loads the full Spring context and tests the complete flow
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = PictureCompressorApplication.class
)
@AutoConfigureWebTestClient
@ActiveProfiles("test")
class GifCompressionIntegrationTest {

    @Autowired
    private WebTestClient webTestClient;

    private static final String TEST_GIF_PATH = "cf2e1e1d-9c07-4dfa-a7ad-350ca90538bb.gif";
    private static final float COMPRESSION_LEVEL = 0.5f;
    
    @BeforeEach
    void setUp() {
        // Configure WebTestClient with increased buffer size for large responses
        webTestClient = webTestClient.mutate()
                .responseTimeout(Duration.ofSeconds(30))
                .codecs(configurer -> configurer
                        .defaultCodecs()
                        .maxInMemorySize(5 * 1024 * 1024)) // 5MB buffer size
                .build();
    }

    /**
     * Test single GIF compression endpoint
     */
    @Test
    void testCompressGif() {
        // Load test GIF file from resources
        Resource testGif = new ClassPathResource(TEST_GIF_PATH);
        
        // Build multipart request
        MultipartBodyBuilder bodyBuilder = new MultipartBodyBuilder();
        bodyBuilder.part("file", testGif)
                .filename("test.gif")
                .contentType(MediaType.IMAGE_GIF);
        
        // Execute request and verify response
        webTestClient.post()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/compress")
                        .queryParam("compressionLevel", COMPRESSION_LEVEL)
                        .build())
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(bodyBuilder.build()))
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.IMAGE_GIF)
                .expectHeader().valueMatches("Content-Disposition", "attachment; filename=\"compressed_.*\\.gif\"")
                .expectBody()
                .consumeWith(response -> {
                    byte[] responseBody = response.getResponseBody();
                    assert responseBody != null && responseBody.length > 0;
                });
    }

    /**
     * Test batch GIF compression endpoint
     */
    @Test
    void testCompressBatch() {
        // Load test GIF file from resources
        Resource testGif = new ClassPathResource(TEST_GIF_PATH);
        
        // Build multipart request with multiple files
        MultipartBodyBuilder bodyBuilder = new MultipartBodyBuilder();
        bodyBuilder.part("files", testGif)
                .filename("test1.gif")
                .contentType(MediaType.IMAGE_GIF);
        bodyBuilder.part("files", testGif)
                .filename("test2.gif")
                .contentType(MediaType.IMAGE_GIF);
        
        // Execute request and verify response
        webTestClient.post()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/compress/batch")
                        .queryParam("compressionLevel", COMPRESSION_LEVEL)
                        .build())
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(bodyBuilder.build()))
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_OCTET_STREAM)
                .expectHeader().valueEquals("Content-Disposition", "attachment; filename=\"compressed_gifs.zip\"")
                .expectBody()
                .consumeWith(response -> {
                    byte[] responseBody = response.getResponseBody();
                    assert responseBody != null && responseBody.length > 0;
                });
    }

    /**
     * Test validation for invalid file type
     */
    @Test
    void testInvalidFileType() throws IOException {
        // Create a temporary text file
        Resource testGif = new ClassPathResource(TEST_GIF_PATH);
        byte[] gifBytes = Files.readAllBytes(testGif.getFile().toPath());
        
        // Build multipart request with invalid file type
        MultipartBodyBuilder bodyBuilder = new MultipartBodyBuilder();
        bodyBuilder.part("file", gifBytes)
                .filename("test.txt") // Wrong extension
                .contentType(MediaType.TEXT_PLAIN);
        
        // Execute request and verify response
        webTestClient.post()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/compress")
                        .queryParam("compressionLevel", COMPRESSION_LEVEL)
                        .build())
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(bodyBuilder.build()))
                .exchange()
                .expectStatus().isBadRequest();
    }

    /**
     * Test validation for invalid compression level
     */
    @Test
    void testInvalidCompressionLevel() {
        // Load test GIF file from resources
        Resource testGif = new ClassPathResource(TEST_GIF_PATH);
        
        // Build multipart request
        MultipartBodyBuilder bodyBuilder = new MultipartBodyBuilder();
        bodyBuilder.part("file", testGif)
                .filename("test.gif")
                .contentType(MediaType.IMAGE_GIF);
        
        // Execute request with invalid compression level and verify response
        webTestClient.post()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/compress")
                        .queryParam("compressionLevel", 1.5f) // Invalid level
                        .build())
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(bodyBuilder.build()))
                .exchange()
                .expectStatus().isBadRequest();
    }
} 