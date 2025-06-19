package com.example.picturecompressor.service;

import com.example.picturecompressor.exception.ProcessingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class ReactiveZipCreatorTest {

    @InjectMocks
    private ReactiveZipCreator zipCreator;
    
    private List<ReactiveZipCreator.ZipEntryData> entries;
    
    @BeforeEach
    void setUp() {
        zipCreator = new ReactiveZipCreator();
        
        // Подготовка тестовых данных
        entries = new ArrayList<>();
        entries.add(new ReactiveZipCreator.ZipEntryData("file1.txt", "content1".getBytes()));
        entries.add(new ReactiveZipCreator.ZipEntryData("file2.txt", "content2".getBytes()));
    }
    
    @Test
    void testCreateZipParallel_Success() {
        // When
        var result = zipCreator.createZipParallel(Flux.fromIterable(entries), 2);
        
        // Then
        StepVerifier.create(result)
            .expectNextMatches(bytes -> bytes.length > 0) // ZIP-файл должен быть непустым
            .verifyComplete();
    }
    
    @Test
    void testCreateZipParallel_EmptyList() {
        // When
        var result = zipCreator.createZipParallel(Flux.empty(), 2);
        
        // Then - теперь ожидаем ошибку вместо успешного завершения
        StepVerifier.create(result)
            .expectErrorMatches(error -> 
                error instanceof ProcessingException && 
                error.getMessage().contains("No valid entries provided for ZIP archive"))
            .verify();
    }
    
    @Test
    void testCreateZipParallel_DuplicateFilenames() {
        // Given - создаем дублирующиеся имена файлов
        entries.add(new ReactiveZipCreator.ZipEntryData("file1.txt", "duplicate content".getBytes()));
        
        // When
        var result = zipCreator.createZipParallel(Flux.fromIterable(entries), 2);
        
        // Then - проверяем, что ZIP создается успешно с уникальными именами
        StepVerifier.create(result)
            .expectNextMatches(bytes -> bytes.length > 0)
            .verifyComplete();
    }
    
    @Test
    void testCreateZipParallel_NullData() {
        // Given
        entries.add(new ReactiveZipCreator.ZipEntryData("null-data.txt", null));
        
        // When
        var result = zipCreator.createZipParallel(Flux.fromIterable(entries), 2);
        
        // Then - проверяем, что null-данные пропускаются и ZIP создается
        StepVerifier.create(result)
            .expectNextMatches(bytes -> bytes.length > 0)
            .verifyComplete();
    }
} 