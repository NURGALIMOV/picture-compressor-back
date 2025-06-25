package com.picturecompressor.service;

import com.picturecompressor.exception.InvalidCompressionLevelException;
import com.picturecompressor.exception.InvalidFileTypeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GifCompressionValidationServiceTest {

    @Test
    void testValidateFileTypeReactive_Success() {
        // Given
        FilePart validFile = mock(FilePart.class);
        when(validFile.filename()).thenReturn("valid.gif");
        
        // When
        Mono<Void> result = GifCompressionValidationService.validateFileTypeReactive(validFile);
        
        // Then
        StepVerifier.create(result)
            .verifyComplete();
    }
    
    @Test
    void testValidateFileTypeReactive_InvalidExtension() {
        // Given
        FilePart invalidFile = mock(FilePart.class);
        when(invalidFile.filename()).thenReturn("invalid.jpg");
        
        // When
        Mono<Void> result = GifCompressionValidationService.validateFileTypeReactive(invalidFile);
        
        // Then
        StepVerifier.create(result)
            .expectError(InvalidFileTypeException.class)
            .verify();
    }
    
    @Test
    void testValidateCompressionLevelReactive_Success() {
        // When - проверка допустимых значений
        StepVerifier.create(GifCompressionValidationService.validateCompressionLevelReactive(0.0f))
            .verifyComplete();
        
        StepVerifier.create(GifCompressionValidationService.validateCompressionLevelReactive(0.5f))
            .verifyComplete();
        
        StepVerifier.create(GifCompressionValidationService.validateCompressionLevelReactive(1.0f))
            .verifyComplete();
    }
    
    @Test
    void testValidateCompressionLevelReactive_TooLow() {
        // When
        Mono<Void> result = GifCompressionValidationService.validateCompressionLevelReactive(-0.1f);
        
        // Then
        StepVerifier.create(result)
            .expectError(InvalidCompressionLevelException.class)
            .verify();
    }
    
    @Test
    void testValidateCompressionLevelReactive_TooHigh() {
        // When
        Mono<Void> result = GifCompressionValidationService.validateCompressionLevelReactive(1.1f);
        
        // Then
        StepVerifier.create(result)
            .expectError(InvalidCompressionLevelException.class)
            .verify();
    }
    
    @Test
    void testValidateCompressionRequest_Success() {
        // Given
        FilePart validFile = mock(FilePart.class);
        when(validFile.filename()).thenReturn("valid.gif");
        float validLevel = 0.5f;
        
        // When
        Mono<Void> result = GifCompressionValidationService.validateCompressionRequest(validFile, validLevel);
        
        // Then
        StepVerifier.create(result)
            .verifyComplete();
    }

    @Test
    void testValidateCompressionRequest_InvalidFile() {
        // Given
        FilePart invalidFile = mock(FilePart.class);
        when(invalidFile.filename()).thenReturn("invalid.jpg");
        float validLevel = 0.5f;
        
        // When
        Mono<Void> result = GifCompressionValidationService.validateCompressionRequest(invalidFile, validLevel);
        
        // Then
        StepVerifier.create(result)
            .expectError(InvalidFileTypeException.class)
            .verify();
    }

    @Test
    void testValidateCompressionRequest_InvalidLevel() {
        // Given
        FilePart validFile = mock(FilePart.class);
        when(validFile.filename()).thenReturn("valid.gif");
        float invalidLevel = 1.5f;
        
        // When
        Mono<Void> result = GifCompressionValidationService.validateCompressionRequest(validFile, invalidLevel);
        
        // Then
        StepVerifier.create(result)
            .expectError(InvalidCompressionLevelException.class)
            .verify();
    }
} 