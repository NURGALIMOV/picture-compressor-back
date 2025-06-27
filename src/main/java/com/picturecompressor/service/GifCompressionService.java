package com.picturecompressor.service;

import com.picturecompressor.config.GifCompressionProperties;
import com.picturecompressor.dto.FileData;
import com.picturecompressor.dto.ZipEntryData;
import com.picturecompressor.exception.FileSizeLimitExceededException;
import com.picturecompressor.exception.InsufficientResourcesException;
import com.picturecompressor.exception.ProcessingException;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.reactor.ratelimiter.operator.RateLimiterOperator;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;

/**
 * Fully non-blocking service for compressing GIF images
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GifCompressionService {

    public static final int ONE_MEGA_BYTE = 1024 * 1024;
    private final GifCompressionProperties props;
    private final ReactiveZipCreator zipCreator;
    private final GifsicleProcessor gifsicleProcessor;

    private final CircuitBreakerOperator<byte[]> circuitBreakerOperator;
    private final BulkheadOperator<byte[]> bulkheadOperator;
    private final RateLimiterOperator<byte[]> rateLimiterOperator;


    /**
     * Check the availability of Gifsicle when starting the application
     */
    @PostConstruct
    public void init() {
        if (Boolean.TRUE.equals(gifsicleProcessor.isGifsicleAvailable())) {
            log.info("Gifsicle is available and will be used for GIF compression");
            return;
        }
        log.error("Gifsicle is not available in the system. Falling back to Java-based compression.");
        throw new IllegalStateException("Gifsicle is not available in the system.");
    }

    /**
     * Compresses a single GIF file with the specified compression level
     *
     * @param filePart The file to compress
     * @param compressionLevel The compression level (0 - 1)
     * @return A Mono containing the compressed GIF file as a byte array
     */
    public Mono<byte[]> compressGif(FilePart filePart, float compressionLevel) {
        return GifCompressionValidationService.validateCompressionRequest(filePart, compressionLevel)
                .doFirst(() -> log.info("Started compressing GIF file: {}", filePart.filename()))
                .then(memoryCheck())
                .then(collectFileContent(filePart))
                .flatMap(this::checkFileMemory)
                .flatMap(bytes -> gifsicleProcessor.compressGif(bytes, compressionLevel))
                .doOnError(e -> log.error("Error compressing GIF file: {}", filePart.filename(), e))
                .doOnSuccess(result -> log.info("Successfully compressed GIF file: {}, output size: {} bytes", filePart.filename(), result.length))
                .transform(circuitBreakerOperator)
                .transform(bulkheadOperator)
                .transform(rateLimiterOperator);
    }

    /**
     * Compresses multiple GIF files with the specified compression level and returns a ZIP archive
     *
     * @param files The files to compress
     * @param compressionLevel The compression level (0-1)
     * @return A Mono containing the compressed GIF files in a ZIP archive as a byte array
     */
    public Mono<byte[]> compressGifBatch(Flux<FilePart> files, float compressionLevel) {
        return GifCompressionValidationService.validateCompressionLevelReactive(compressionLevel)
                .doFirst(() -> log.info("Started batch compression"))
                .then(memoryCheck())
                .then(processFiles(files))
                .flatMap(this::checkTotalMemoryRequirements)
                .flatMap(fileDataList -> compressFiles(fileDataList, compressionLevel))
                .flatMap(this::createZipArchive)
                .doOnError(e -> log.error("Error in batch compression of GIF files", e))
                .doOnSuccess(result -> log.info("Finished batch compression of GIF files, output size: {} bytes", result.length))
                .transform(circuitBreakerOperator)
                .transform(bulkheadOperator)
                .transform(rateLimiterOperator);
    }

    /**
     * Processes incoming file parts into FileData objects
     */
    private Mono<List<FileData>> processFiles(Flux<FilePart> files) {
        return files.flatMap(this::processFile)
                .collectList()
                .flatMap(fileDataList -> {
                    if (fileDataList.isEmpty()) {
                        return Mono.error(new ProcessingException("No valid files provided for compression"));
                    }
                    log.info("Processing {} files for batch compression", fileDataList.size());
                    return Mono.just(fileDataList);
                });
    }

    private Mono<FileData> processFile(FilePart filePart) {
        return GifCompressionValidationService.validateFileTypeReactive(filePart)
                .then(collectFileContent(filePart))
                .map(bytes -> new FileData(filePart.filename(), bytes))
                .doOnSuccess(fileData -> log.debug("Successfully read file: {}, size: {} bytes", fileData.filename(), fileData.data().length))
                .onErrorResume(e -> {
                    log.error("Error processing file: {}, skipping", filePart.filename(), e);
                    return Mono.empty();
                });
    }

    /**
     * Compresses the list of FileData objects with the specified compression level
     */
    private Mono<List<ZipEntryData>> compressFiles(List<FileData> fileDataList, float compressionLevel) {
        int concurrency = Math.max(1, Runtime.getRuntime().availableProcessors());
        return Flux.fromIterable(fileDataList)
                .flatMap(fileData -> fileDataToZipEntry(compressionLevel, fileData), concurrency)
                .collectList()
                .flatMap(zipEntries ->
                        zipEntries.isEmpty() ? Mono.error(new ProcessingException("No valid files could be compressed")) : Mono.just(zipEntries)
                );
    }

    private Mono<ZipEntryData> fileDataToZipEntry(float compressionLevel, FileData fileData) {
        return gifsicleProcessor.compressGif(fileData.data(), compressionLevel)
                .map(compressedData -> new ZipEntryData(fileData.filename(), compressedData))
                .onErrorResume(e -> {
                    log.error("Error compressing file: {}. Skipping invalid file", fileData.filename(), e);
                    return Mono.empty();
                });
    }

    /**
     * Creates a ZIP archive from the list of compressed files
     */
    private Mono<byte[]> createZipArchive(List<ZipEntryData> zipEntries) {
        log.info("Creating ZIP archive with {} compressed files", zipEntries.size());
        List<ZipEntryData> validEntries = filterValidEntries(zipEntries);
        if (validEntries.isEmpty()) {
            return Mono.error(new ProcessingException("No valid data to include in ZIP archive"));
        }
        int parallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
        return zipCreator.createZipParallel(Flux.fromIterable(validEntries), parallelism)
                .doOnSuccess(zipBytes -> log.info("ZIP archive created successfully, size: {} bytes", zipBytes.length));
    }

    /**
     * Filters out invalid entries from the list of zip entries
     */
    private List<ZipEntryData> filterValidEntries(List<ZipEntryData> zipEntries) {
        boolean hasInvalidEntries = zipEntries.stream().anyMatch(entry -> entry.data() == null || entry.data().length == 0);
        if (hasInvalidEntries) {
            log.warn("Some entries in ZIP have no data, removing them");
            List<ZipEntryData> validEntries = new ArrayList<>(zipEntries);
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
        return DataBufferUtils.join(filePart.content()).flatMap(dataBuffer -> {
            try {
                int readableBytes = dataBuffer.readableByteCount();
                if (readableBytes > props.getMaxFileSize()) {
                    return Mono.error(new FileSizeLimitExceededException("File size exceeds the maximum allowed size of %s bytes".formatted(props.getMaxFileSize())));
                }
                byte[] bytes = new byte[readableBytes];
                dataBuffer.read(bytes);
                return Mono.just(bytes);
            } finally {
                DataBufferUtils.release(dataBuffer);
            }
        });
    }

    /**
     * Check system resources before heavy ops (optional).
     */
    private Mono<Void> memoryCheck() {
        if (props.isMemoryCheckEnabled()) {
            return Mono.fromCallable(() -> {
                double used = getUsedMemoryPercent();
                if (used > props.getMaxMemoryPercent()) {
                    throw new InsufficientResourcesException("Low memory: %.1f%% used (limit %.1f%%)".formatted(used, props.getMaxMemoryPercent()));
                }
                return null;
            }).subscribeOn(Schedulers.boundedElastic()).then();
        }
        return Mono.empty();
    }

    private double getUsedMemoryPercent() {
        Runtime r = Runtime.getRuntime();
        long max = r.maxMemory();
        long used = r.totalMemory() - r.freeMemory();
        return ((double) used / max) * 100;
    }

    /**
     * For single file: check estimated memory if check enabled.
     */
    private Mono<byte[]> checkFileMemory(byte[] bytes) {
        if (props.isMemoryCheckEnabled()) {
            long estimated = (long) bytes.length * props.getEstimatedMemoryFactor();
            long available = getAvailableMemory();
            if (estimated > available * 0.8) {
                String message = "File too large for current memory: need ~%dMB, have %dMB".formatted(estimated / ONE_MEGA_BYTE, available / ONE_MEGA_BYTE);
                return Mono.error(new InsufficientResourcesException(message));
            }
        }
        return Mono.just(bytes);
    }

    private long getAvailableMemory() {
        Runtime r = Runtime.getRuntime();
        return r.maxMemory() - (r.totalMemory() - r.freeMemory());
    }

    /**
     * Checks the common memory necessary for processing a file package
     */
    private Mono<List<FileData>> checkTotalMemoryRequirements(List<FileData> files) {
        if (files.size() > props.getMaxBatchSize()) {
            String message = "Batch size %d exceeds max %d".formatted(files.size(), props.getMaxBatchSize());
            return Mono.error(new InsufficientResourcesException(message));
        }
        if (props.isMemoryCheckEnabled()) {
            long totalSize = files.stream().mapToLong(file -> file.data().length).sum();
            long estimatedMemoryRequired = totalSize * props.getEstimatedMemoryFactor();
            long availableMem = getAvailableMemory();
            if (estimatedMemoryRequired > availableMem * 0.8) {
                String message = "Batch is too large to process with current system resources. Try processing fewer or smaller files.";
                return Mono.error(new InsufficientResourcesException(message));
            }
            return Mono.just(files);
        }
        return Mono.just(files);
    }
}