package com.picturecompressor.service;

import com.picturecompressor.config.HealthConfig;
import com.picturecompressor.exception.FileSizeLimitExceededException;
import com.picturecompressor.exception.InsufficientResourcesException;
import com.picturecompressor.exception.ProcessingException;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.reactor.ratelimiter.operator.RateLimiterOperator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.annotation.PostConstruct;

/**
 * Fully non-blocking service for compressing GIF images
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GifCompressionService {

    private static final String COMPRESSION_SERVICE = "compressionService";
    private final ReactiveGifProcessor gifProcessor;
    private final ReactiveZipCreator zipCreator;
    private final GifsicleProcessor gifsicleProcessor;
    
    // Resilience4j operators
    private final CircuitBreakerOperator<byte[]> circuitBreakerOperator;
    private final BulkheadOperator<byte[]> bulkheadOperator;
    private final RateLimiterOperator<byte[]> rateLimiterOperator;
    
    @Value("${gif-compression.max-file-size}")
    private long maxFileSize;
    
    @Value("${gif-compression.max-memory-percent:80}")
    private int maxMemoryPercent;
    
    @Value("${gif-compression.estimated-memory-factor:3.5}")
    private float estimatedMemoryFactor;
    
    @Value("${gif-compression.constrained-mode:false}")
    private boolean constrainedMode;
    
    @Value("${gif-compression.max-batch-size:5}")
    private int maxBatchSize;
    
    @Value("${gifsicle.enabled:false}")
    private boolean gifsicleEnabled;
    
    private boolean gifsicleAvailable = false;
    
    @PostConstruct
    public void init() {
        // Проверяем доступность gifsicle при старте приложения
        if (gifsicleEnabled) {
            gifsicleProcessor.isGifsicleAvailable()
                .doOnNext(available -> {
                    gifsicleAvailable = available;
                    if (Boolean.TRUE.equals(available)) {
                        log.info("Gifsicle is available and will be used for GIF compression");
                    } else {
                        log.warn("Gifsicle is enabled in configuration but not available in the system. " +
                                "Falling back to Java-based compression.");
                    }
                })
                .subscribe();
        } else {
            log.info("Gifsicle is disabled in configuration. Using Java-based compression.");
        }
    }

    /**
     * Compresses a single GIF file with the specified compression level
     *
     * @param filePart The file to compress
     * @param compressionLevel The compression level (0 - 1)
     * @return A Mono containing the compressed GIF file as a byte array
     */
    @CircuitBreaker(name = COMPRESSION_SERVICE, fallbackMethod = "compressGifFallback")
    @Bulkhead(name = COMPRESSION_SERVICE, fallbackMethod = "compressGifFallback")
    public Mono<byte[]> compressGif(FilePart filePart, float compressionLevel) {
        return GifCompressionValidationService.validateCompressionRequest(filePart, compressionLevel)
                .then(checkSystemResources())
                .then(collectFileContent(filePart))
                .doFirst(() -> log.info("Started compressing GIF file: {}", filePart.filename()))
                .flatMap(bytes -> checkMemoryForProcessing(bytes).then(Mono.just(bytes)))
                .flatMap(bytes -> {
                    // Используем gifsicle, если доступен и включен, иначе используем Java-решение
                    if (gifsicleEnabled && gifsicleAvailable) {
                        log.info("Using gifsicle for file: {}", filePart.filename());
                        return gifsicleProcessor.compressGif(bytes, compressionLevel)
                                .onErrorResume(e -> {
                                    log.warn("Gifsicle processing failed, falling back to Java implementation: {}", e.getMessage());
                                    return processWithJava(bytes, compressionLevel);
                                });
                    }
                    return processWithJava(bytes, compressionLevel);
                })
                .doOnError(e -> log.error("Error compressing GIF file: {}", filePart.filename(), e))
                .doOnSuccess(
                        result -> log.info("Successfully compressed GIF file: {}, output size: {} bytes", filePart.filename(), result.length)
                )
                .transform(circuitBreakerOperator)
                .transform(bulkheadOperator)
                .transform(rateLimiterOperator)
                .onErrorMap(this::mapResourceErrors);
    }

    /**
     * Обрабатывает GIF встроенными средствами Java
     */
    private Mono<byte[]> processWithJava(byte[] bytes, float compressionLevel) {
        if (constrainedMode) {
            log.info("Using constrained Java mode for GIF compression");
            return gifProcessor.compressGif(bytes, compressionLevel);
        } else {
            return gifProcessor.compressGifParallel(bytes, compressionLevel);
        }
    }

    /**
     * Fallback method for compressGif when circuit breaker opens or bulkhead rejects
     */
    private Mono<byte[]> compressGifFallback(FilePart filePart, float compressionLevel, Exception e) {
        log.warn("Compression fallback triggered for file: {}, reason: {}", filePart.filename(), e.getMessage());
        return Mono.error(new InsufficientResourcesException("Service temporarily unavailable due to high load. Please try again later.", e));
    }

    /**
     * Compresses multiple GIF files with the specified compression level and returns a ZIP archive
     *
     * @param files The files to compress
     * @param compressionLevel The compression level (0-1)
     * @return A Mono containing the compressed GIF files in a ZIP archive as a byte array
     */
    @CircuitBreaker(name = COMPRESSION_SERVICE, fallbackMethod = "compressBatchFallback")
    @Bulkhead(name = COMPRESSION_SERVICE, fallbackMethod = "compressBatchFallback")
    public Mono<byte[]> compressGifBatch(Flux<FilePart> files, float compressionLevel) {
        return GifCompressionValidationService.validateCompressionLevelReactive(compressionLevel)
                .then(checkSystemResources())
                .then(processFiles(files))
                .flatMap(fileDataList -> {
                    if (constrainedMode && fileDataList.size() > maxBatchSize) {
                        log.warn("Batch size {} exceeds maximum allowed size {} in constrained mode", fileDataList.size(), maxBatchSize);
                        return Mono.error(
                                new InsufficientResourcesException(
                                        "Maximum batch size exceeded. Please reduce the number of files to " + maxBatchSize + " or less."
                                )
                        );
                    }
                    return checkTotalMemoryRequirements(fileDataList).thenReturn(fileDataList);
                })
                .flatMap(fileDataList -> compressFiles(fileDataList, compressionLevel))
                .flatMap(this::createZipArchive)
                .doOnError(e -> log.error("Error in batch compression of GIF files", e))
                .doOnSuccess(result -> log.info("Finished batch compression of GIF files, output size: {} bytes", result.length))
                .transform(circuitBreakerOperator)
                .transform(bulkheadOperator)
                .transform(rateLimiterOperator)
                .onErrorMap(this::mapResourceErrors);
    }
    
    /**
     * Fallback method for compressBatch when circuit breaker opens or bulkhead rejects
     */
    private Mono<byte[]> compressBatchFallback(Flux<FilePart> files, float compressionLevel, Exception e) {
        log.warn("Batch compression fallback triggered, reason: {}", e.getMessage());
        return Mono.error(new InsufficientResourcesException("Service temporarily unavailable due to high load. Please try again later.", e));
    }
    
    /**
     * Map resource-related errors to more specific exceptions
     */
    private Throwable mapResourceErrors(Throwable e) {
        return e;
    }

    /**
     * Processes incoming file parts into FileData objects
     */
    private Mono<List<FileData>> processFiles(Flux<FilePart> files) {
        return files.flatMap(filePart ->
                    GifCompressionValidationService.validateFileTypeReactive(filePart)
                            .then(collectFileContent(filePart))
                            .map(bytes -> new FileData(filePart.filename(), bytes))
                            .doOnSuccess(fileData -> log.debug("Successfully read file: {}, size: {} bytes", fileData.filename(), fileData.data().length))
                            .onErrorResume(e -> {
                                log.error("Error processing file: {}, skipping", filePart.filename(), e);
                                return Mono.empty();
                            })
                )
                .doFirst(() -> log.info("Started batch compression of GIF files"))
                .collectList()
                .flatMap(fileDataList -> {
                    if (fileDataList.isEmpty()) {
                        return Mono.error(new ProcessingException("No valid files provided for compression"));
                    }
                    log.info("Processing {} files for batch compression", fileDataList.size());
                    return Mono.just(fileDataList);
                });
    }

    /**
     * Compresses the list of FileData objects with the specified compression level
     */
    private Mono<List<ReactiveZipCreator.ZipEntryData>> compressFiles(List<FileData> fileDataList, float compressionLevel) {
        return Flux.fromIterable(fileDataList)
                .flatMap(fileData -> 
                    compressSingleFile(fileData.data(), compressionLevel)
                        .map(compressedData -> new ReactiveZipCreator.ZipEntryData(fileData.filename(), compressedData))
                        .onErrorResume(e -> {
                            log.error("Error compressing file: {}. Skipping invalid file", fileData.filename(), e);
                            return Mono.empty();
                        }),
                    constrainedMode ? 1 : 4
                )
                .collectList()
                .flatMap(zipEntries -> {
                    if (zipEntries.isEmpty()) {
                        return Mono.error(new ProcessingException("No valid files could be compressed"));
                    }
                    return Mono.just(zipEntries);
                });
    }
    
    /**
     * Сжимает один файл, используя gifsicle или Java-реализацию
     */
    private Mono<byte[]> compressSingleFile(byte[] fileData, float compressionLevel) {
        if (gifsicleEnabled && gifsicleAvailable) {
            return gifsicleProcessor.compressGif(fileData, compressionLevel)
                    .onErrorResume(e -> {
                        log.warn("Gifsicle processing failed, falling back to Java implementation: {}", e.getMessage());
                        return processWithJava(fileData, compressionLevel);
                    });
        }
        return processWithJava(fileData, compressionLevel);
    }

    /**
     * Creates a ZIP archive from the list of compressed files
     */
    private Mono<byte[]> createZipArchive(List<ReactiveZipCreator.ZipEntryData> zipEntries) {
        log.info("Creating ZIP archive with {} compressed files", zipEntries.size());
        List<ReactiveZipCreator.ZipEntryData> validEntries = filterValidEntries(zipEntries);
        if (validEntries.isEmpty()) {
            return Mono.error(new ProcessingException("No valid data to include in ZIP archive"));
        }
        
        // Use sequential ZIP creation in constrained mode
        int parallelism = constrainedMode ? 1 : 4;
        log.debug("Using parallelism level {} for ZIP creation", parallelism);
        
        return zipCreator.createZipParallel(Flux.fromIterable(validEntries), parallelism)
                .doOnSuccess(zipBytes -> log.info("ZIP archive created successfully, size: {} bytes", zipBytes.length));
    }
    
    /**
     * Filters out invalid entries from the list of zip entries
     */
    private List<ReactiveZipCreator.ZipEntryData> filterValidEntries(List<ReactiveZipCreator.ZipEntryData> zipEntries) {
        boolean hasInvalidEntries = zipEntries.stream().anyMatch(entry -> entry.data() == null || entry.data().length == 0);
        if (hasInvalidEntries) {
            log.warn("Some entries in ZIP have no data, removing them");
            List<ReactiveZipCreator.ZipEntryData> validEntries = new ArrayList<>(zipEntries);
            validEntries.removeIf(entry -> entry.data() == null || entry.data().length == 0);
            return validEntries;
        }
        return zipEntries;
    }

    /**
     * Collects file content into a byte array with size validation using reactive patterns
     * 
     * @param filePart The file part to collect
     * @return A Mono containing the file bytes
     */
    private Mono<byte[]> collectFileContent(FilePart filePart) {
        final AtomicLong size = new AtomicLong(0);
        return DataBufferUtils.join(filePart.content())
                .flatMap(dataBuffer -> {
                    try (DataBuffer.ByteBufferIterator iterator = dataBuffer.readableByteBuffers()) {
                        List<ByteBuffer> buffers = new ArrayList<>();
                        iterator.forEachRemaining(buffer -> buffers.add(buffer.duplicate()));
                        ByteBuffer[] byteBuffers = buffers.toArray(ByteBuffer[]::new);

                        int totalBytes = Arrays.stream(byteBuffers).mapToInt(ByteBuffer::remaining).sum();
                        byte[] bytes = new byte[totalBytes];

                        int offset = 0;
                        for (ByteBuffer byteBuffer : byteBuffers) {
                            int length = byteBuffer.remaining();
                            byteBuffer.get(bytes, offset, length);
                            offset += length;
                        }

                        long fileSize = size.addAndGet(bytes.length);
                        if (fileSize > maxFileSize) {
                            return Mono.error(new FileSizeLimitExceededException("File size exceeds the maximum allowed size of " + maxFileSize + " bytes"));
                        }
                        return Mono.just(bytes);
                    } finally {
                        DataBufferUtils.release(dataBuffer);
                    }
        });
    }

    /**
     * Record to hold file data for batch processing
     */
    private record FileData(String filename, byte[] data) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FileData fileData = (FileData) o;
            return Objects.equals(filename, fileData.filename) &&
                    Arrays.equals(data, fileData.data);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(filename);
            result = 31 * result + Arrays.hashCode(data);
            return result;
        }

        @Override
        public String toString() {
            return "FileData{" +
                    "filename='" + filename + '\'' +
                    ", data=" + Arrays.toString(data) +
                    '}';
        }
    }

    /**
     * Проверяет доступные системные ресурсы перед обработкой
     * @return Mono<Void> завершающийся успешно, если ресурсов достаточно, или с ошибкой, если нет
     */
    private Mono<Void> checkSystemResources() {
        return Mono.fromCallable(() -> {
            Runtime runtime = Runtime.getRuntime();
            long maxMem = runtime.maxMemory();
            long freeMem = runtime.freeMemory();
            long totalMem = runtime.totalMemory();
            
            // Доступная память = максимальная - (используемая - свободная)
            long availableMem = maxMem - (totalMem - freeMem);
            int usedMemoryPercent = (int) ((maxMem - availableMem) / maxMem * 100);
            
            log.debug("Memory status: used={}%, available={}MB, max={}MB", 
                usedMemoryPercent, availableMem / HealthConfig.MB_DIVISOR, maxMem / HealthConfig.MB_DIVISOR);
                
            // More conservative threshold for constrained environments
            int effectiveMemoryThreshold = constrainedMode ? maxMemoryPercent - 10 : maxMemoryPercent;
                
            if (usedMemoryPercent > effectiveMemoryThreshold) {
                log.warn("Low memory resources detected: {}% used (threshold: {}%)", usedMemoryPercent, effectiveMemoryThreshold);
                return new InsufficientResourcesException("Service is low on memory resources. Please try again later.");
            }
            return null;
        })
        .flatMap(exception -> Objects.isNull(exception) ? Mono.<Void>empty() : Mono.error(exception))
        .subscribeOn(Schedulers.boundedElastic());
    }
    
    /**
     * Оценивает, хватит ли памяти для обработки данного GIF-файла
     * Коэффициент 3.5 основан на том, что декодированное изображение требует примерно в 3-4 раза
     * больше памяти, чем исходный файл (в зависимости от формата и сжатия)
     */
    private Mono<Void> checkMemoryForProcessing(byte[] fileBytes) {
        return Mono.fromCallable(() -> {
            long estimatedMemoryRequired = (long)(fileBytes.length * estimatedMemoryFactor);
            Runtime runtime = Runtime.getRuntime();
            long availableMem = runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory());
            
            // Additional safety margin for constrained environments
            double safetyFactor = constrainedMode ? 0.6 : 0.8;
            
            // Проверяем, что у нас есть достаточно памяти для обработки файла
            if (estimatedMemoryRequired > availableMem * safetyFactor) {
                log.warn("Insufficient memory to safely process file: need ~{}MB, available {}MB", 
                    estimatedMemoryRequired / HealthConfig.MB_DIVISOR, availableMem / HealthConfig.MB_DIVISOR);
                return new InsufficientResourcesException(
                    "File is too large to process with current system resources. Try again with a smaller file or when system has more available memory.");
            }
            return null;
        })
        .flatMap(exception -> Objects.isNull(exception) ? Mono.<Void>empty() : Mono.error(exception))
        .subscribeOn(Schedulers.boundedElastic());
    }
    
    /**
     * Проверяет общую память, необходимую для обработки пакета файлов
     */
    private Mono<Void> checkTotalMemoryRequirements(List<FileData> files) {
        return Mono.<InsufficientResourcesException>fromCallable(() -> {
            long totalSize = files.stream().mapToLong(file -> file.data().length).sum();
            long estimatedMemoryRequired = (long)(totalSize * estimatedMemoryFactor);
            
            Runtime runtime = Runtime.getRuntime();
            long availableMem = runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory());
            
            // Additional safety margin for constrained environments
            double safetyFactor = constrainedMode ? 0.6 : 0.8;
            
            if (estimatedMemoryRequired > availableMem * safetyFactor) {
                log.warn("Insufficient memory for batch processing: need ~{}MB, available {}MB", 
                    estimatedMemoryRequired / (1024 * 1024), availableMem / (1024 * 1024));
                return new InsufficientResourcesException(
                    "Batch is too large to process with current system resources. Try processing fewer or smaller files.");
            }
            
            return null;
        })
        .flatMap(exception -> exception == null ? Mono.<Void>empty() : Mono.error(exception))
        .subscribeOn(Schedulers.boundedElastic());
    }
}