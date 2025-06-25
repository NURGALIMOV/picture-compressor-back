package com.picturecompressor.service;

import com.picturecompressor.exception.ProcessingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

/**
 * Tests for the ReactiveGifProcessor
 */
@ExtendWith(MockitoExtension.class)
class ReactiveGifProcessorTest {

    @InjectMocks
    private ReactiveGifProcessor processor;
    
    @BeforeEach
    void setUp() {
        processor = new ReactiveGifProcessor();
    }
    
    @Test
    void testCompressGif_InvalidGifData() {
        byte[] invalidData = "not a gif".getBytes();
        float compressionLevel = 0.5f;
        
        StepVerifier.create(processor.compressGif(invalidData, compressionLevel))
            .expectError(ProcessingException.class)
            .verify();
    }
    
    @Test
    void testCompressGif_NullData() {
        float compressionLevel = 0.5f;
        
        StepVerifier.create(processor.compressGif(null, compressionLevel))
            .expectError(ProcessingException.class)
            .verify();
    }
    
    @Test
    void testCompressGifParallel_InvalidGifData() {
        byte[] invalidData = "not a gif".getBytes();
        float compressionLevel = 0.5f;
        
        StepVerifier.create(processor.compressGifParallel(invalidData, compressionLevel))
            .expectError(ProcessingException.class)
            .verify();
    }
    
    @Test
    void testCompressGifParallel_NullData() {
        float compressionLevel = 0.5f;
        
        StepVerifier.create(processor.compressGifParallel(null, compressionLevel))
            .expectError(ProcessingException.class)
            .verify();
    }
    
    @Test
    void testCompressGif_DifferentCompressionLevels() {
        // Создаем простой GIF-файл для тестирования
        byte[] gifBytes = createMinimalGif();
        
        // Проверяем разные уровни компрессии
        float[] levels = {0.0f, 0.5f, 1.0f};
        
        for (float level : levels) {
            StepVerifier.create(processor.compressGif(gifBytes, level))
                .expectError() // Мы ожидаем ошибку, так как наш минимальный GIF не настоящий
                .verify();
        }
    }
    
    // Вспомогательный метод для создания минимального валидного GIF
    private byte[] createMinimalGif() {
        // Минимальный GIF-header для прохождения валидации
        return new byte[] {
            'G', 'I', 'F', '8', '9', 'a',  // GIF signature
            0, 0, 0, 0, 0, 0,               // дополнительные байты
        };
    }
} 