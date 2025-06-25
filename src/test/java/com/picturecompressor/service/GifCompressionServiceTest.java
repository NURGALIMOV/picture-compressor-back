package com.picturecompressor.service;

import com.picturecompressor.exception.FileSizeLimitExceededException;
import com.picturecompressor.exception.InvalidCompressionLevelException;
import com.picturecompressor.exception.InvalidFileTypeException;
import com.picturecompressor.exception.ProcessingException;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.reactor.ratelimiter.operator.RateLimiterOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GifCompressionServiceTest {

    @Mock
    private ReactiveGifProcessor gifProcessor;

    @Mock
    private ReactiveZipCreator zipCreator;
    
    @Mock
    private GifsicleProcessor gifsicleProcessor;

    private GifCompressionService compressionService;

    private FilePart mockFilePart;
    private final byte[] compressedGif = "mock compressed gif data".getBytes();
    private final byte[] zipData = "mock zip data".getBytes();

    @BeforeEach
    @SuppressWarnings({"unchecked", "rawtypes"})
    void setUp() throws Exception {
        // Создание мок-объектов для Resilience4j операторов
        CircuitBreakerOperator<byte[]> circuitBreakerOperator = mock(CircuitBreakerOperator.class);
        BulkheadOperator<byte[]> bulkheadOperator = mock(BulkheadOperator.class);
        RateLimiterOperator<byte[]> rateLimiterOperator = mock(RateLimiterOperator.class);
        
        // Настройка поведения моков-операторов (просто пропускают значения)
        when(circuitBreakerOperator.apply(any())).thenAnswer(i -> i.getArgument(0));
        when(bulkheadOperator.apply(any())).thenAnswer(i -> i.getArgument(0));
        when(rateLimiterOperator.apply(any())).thenAnswer(i -> i.getArgument(0));
        
        // Создаем сервис вручную с макетами зависимостей
        compressionService = new GifCompressionService(gifProcessor, zipCreator, gifsicleProcessor,
                circuitBreakerOperator, bulkheadOperator, rateLimiterOperator);

        // Устанавливаем maxFileSize через рефлексию
        Field field = GifCompressionService.class.getDeclaredField("maxFileSize");
        field.setAccessible(true);
        field.set(compressionService, 1024 * 1024); // 1MB
        
        // Устанавливаем gifsicleEnabled в false для тестов
        Field gifsicleEnabledField = GifCompressionService.class.getDeclaredField("gifsicleEnabled");
        gifsicleEnabledField.setAccessible(true);
        gifsicleEnabledField.set(compressionService, false); // По умолчанию используем Java-реализацию в тестах

        // Настраиваем моки
        when(gifProcessor.compressGifParallel(any(byte[].class), anyFloat()))
                .thenReturn(Mono.just(compressedGif));
        
        when(gifProcessor.compressGif(any(byte[].class), anyFloat()))
                .thenReturn(Mono.just(compressedGif));
                
        // Настраиваем мок для gifsicleProcessor
        when(gifsicleProcessor.compressGif(any(byte[].class), anyFloat()))
                .thenReturn(Mono.just(compressedGif));
        
        when(gifsicleProcessor.isGifsicleAvailable())
                .thenReturn(Mono.just(true));

        // Настраиваем мок zipCreator
        when(zipCreator.createZipParallel(any(Flux.class), anyInt()))
                .thenReturn(Mono.just(zipData));

        // Создаем мок FilePart
        mockFilePart = mock(FilePart.class);

        // Создаем тестовые данные GIF
        byte[] gifHeader = { 'G', 'I', 'F', '8', '9', 'a' }; // GIF header
        DataBuffer mockDataBuffer = new DefaultDataBufferFactory().wrap(gifHeader);

        // Настраиваем мок
        when(mockFilePart.filename()).thenReturn("test.gif");
        when(mockFilePart.content()).thenReturn(Flux.just(mockDataBuffer));
    }

    @Test
    void testCompressGif_Success() {
        // When
        Mono<byte[]> result = compressionService.compressGif(mockFilePart, 0.5f);
        
        // Then
        StepVerifier.create(result)
                .expectNext(compressedGif)
                .verifyComplete();
        
        verify(gifProcessor).compressGifParallel(any(byte[].class), eq(0.5f));
    }
    
    @Test
    void testCompressGif_WithGifsicle() throws Exception {
        // Given
        Field gifsicleEnabledField = GifCompressionService.class.getDeclaredField("gifsicleEnabled");
        gifsicleEnabledField.setAccessible(true);
        gifsicleEnabledField.set(compressionService, true);
        
        Field gifsicleAvailableField = GifCompressionService.class.getDeclaredField("gifsicleAvailable");
        gifsicleAvailableField.setAccessible(true);
        gifsicleAvailableField.set(compressionService, true);
        
        // When
        Mono<byte[]> result = compressionService.compressGif(mockFilePart, 0.5f);
        
        // Then
        StepVerifier.create(result)
                .expectNext(compressedGif)
                .verifyComplete();
        
        verify(gifsicleProcessor).compressGif(any(byte[].class), eq(0.5f));
        verify(gifProcessor, never()).compressGifParallel(any(byte[].class), anyFloat());
    }

    @Test
    void validateFileTypeSuccess() {
        // When validateFileType is called implicitly
        Mono<byte[]> result = compressionService.compressGif(mockFilePart, 0.5f);

        // Then no exception is thrown
        StepVerifier.create(result)
                .expectNext(compressedGif)
                .verifyComplete();
    }

    @Test
    void validateFileTypeFailure() {
        // Given
        FilePart invalidFilePart = mock(FilePart.class);
        when(invalidFilePart.filename()).thenReturn("test.jpg");
        
        // Create a mock content with non-GIF bytes
        byte[] nonGifBytes = { 'J', 'P', 'E', 'G' }; // Non-GIF header
        DataBuffer mockDataBuffer = new DefaultDataBufferFactory().wrap(nonGifBytes);
        when(invalidFilePart.content()).thenReturn(Flux.just(mockDataBuffer));
        
        // When
        Mono<byte[]> result = compressionService.compressGif(invalidFilePart, 0.5f);
        
        // Then
        StepVerifier.create(result)
                .expectError(InvalidFileTypeException.class)
                .verify();
    }

    @Test
    void validateCompressionLevelSuccess() {
        // Given a valid compression level
        float validLevel = 0.5f;

        // When compression service is called with valid level
        Mono<byte[]> result = compressionService.compressGif(mockFilePart, validLevel);

        // Then no exception is thrown
        StepVerifier.create(result)
                .expectNext(compressedGif)
                .verifyComplete();
    }

    @Test
    void validateCompressionLevelTooLow() {
        // Given an invalid compression level
        float invalidLevel = -0.1f;
        
        // When
        Mono<byte[]> result = compressionService.compressGif(mockFilePart, invalidLevel);
        
        // Then
        StepVerifier.create(result)
            .expectError(InvalidCompressionLevelException.class)
            .verify();
    }

    @Test
    void validateCompressionLevelTooHigh() {
        // Given an invalid compression level
        float invalidLevel = 1.1f;
        
        // When
        Mono<byte[]> result = compressionService.compressGif(mockFilePart, invalidLevel);
        
        // Then
        StepVerifier.create(result)
            .expectError(InvalidCompressionLevelException.class)
            .verify();
    }
    
    @Test
    void testCompressGifBatch_Success() {
        // Given
        Flux<FilePart> fileParts = Flux.just(mockFilePart, mockFilePart);
        
        // When
        Mono<byte[]> result = compressionService.compressGifBatch(fileParts, 0.5f);
        
        // Then
        StepVerifier.create(result)
                .expectNext(zipData)
                .verifyComplete();
                
        verify(zipCreator).createZipParallel(any(Flux.class), anyInt());
    }
    
    @Test
    void testCompressGifBatch_EmptyFlux() {
        // Given
        Flux<FilePart> emptyFlux = Flux.empty();
        
        // When
        Mono<byte[]> result = compressionService.compressGifBatch(emptyFlux, 0.5f);
        
        // Then
        StepVerifier.create(result)
                .expectError(ProcessingException.class)
                .verify();
    }
    
    @Test
    void fileSizeLimitExceeded() {
        // Given a file that exceeds the size limit
        byte[] largeContent = new byte[2 * 1024 * 1024]; // 2MB data (exceeds 1MB limit)
        DataBuffer largeBuffer = new DefaultDataBufferFactory().wrap(largeContent);
        
        // Create a special mock for this test case
        FilePart largeMockFilePart = mock(FilePart.class);
        when(largeMockFilePart.filename()).thenReturn("large.gif");
        when(largeMockFilePart.content()).thenReturn(Flux.just(largeBuffer));

        // When compression service is called with the large file
        Mono<byte[]> result = compressionService.compressGif(largeMockFilePart, 0.5f);

        // Then a FileSizeLimitExceededException is thrown
        StepVerifier.create(result)
                .expectError(FileSizeLimitExceededException.class)
                .verify();
    }
}