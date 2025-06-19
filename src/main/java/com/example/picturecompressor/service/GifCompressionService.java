package com.example.picturecompressor.service;

import com.example.picturecompressor.exception.FileSizeLimitExceededException;
import com.example.picturecompressor.exception.ProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fully non-blocking service for compressing GIF images
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GifCompressionService {

    private final ReactiveGifProcessor gifProcessor;
    private final ReactiveZipCreator zipCreator;
    
    @Value("${gif-compression.max-file-size}")
    private long maxFileSize;

    /**
     * Compresses a single GIF file with the specified compression level
     *
     * @param filePart The file to compress
     * @param compressionLevel The compression level (0 - 1)
     * @return A Mono containing the compressed GIF file as a byte array
     */
    public Mono<byte[]> compressGif(FilePart filePart, float compressionLevel) {
        return GifCompressionValidationService.validateCompressionRequest(filePart, compressionLevel)
                .then(collectFileContent(filePart))
                .doFirst(() -> log.info("Started compressing GIF file: {}", filePart.filename()))
                .flatMap(bytes -> gifProcessor.compressGifParallel(bytes, compressionLevel))
                .doOnError(e -> log.error("Error compressing GIF file: {}", filePart.filename(), e))
                .doOnSuccess(
                        result -> log.info("Successfully compressed GIF file: {}, output size: {} bytes", filePart.filename(), result.length)
                );
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
                .then(processFiles(files))
                .flatMap(fileDataList -> compressFiles(fileDataList, compressionLevel))
                .flatMap(this::createZipArchive)
                .doOnError(e -> log.error("Error in batch compression of GIF files", e))
                .doOnSuccess(result -> log.info("Finished batch compression of GIF files, output size: {} bytes", result.length));
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
                    gifProcessor.compressGif(fileData.data(), compressionLevel)
                        .map(compressedData -> new ReactiveZipCreator.ZipEntryData(fileData.filename(), compressedData))
                        .onErrorResume(e -> {
                            log.error("Error compressing file: {}. Skipping invalid file", fileData.filename(), e);
                            return Mono.empty();
                        })
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
     * Creates a ZIP archive from the list of compressed files
     */
    private Mono<byte[]> createZipArchive(List<ReactiveZipCreator.ZipEntryData> zipEntries) {
        log.info("Creating ZIP archive with {} compressed files", zipEntries.size());
        List<ReactiveZipCreator.ZipEntryData> validEntries = filterValidEntries(zipEntries);
        if (validEntries.isEmpty()) {
            return Mono.error(new ProcessingException("No valid data to include in ZIP archive"));
        }
        return zipCreator.createZipParallel(Flux.fromIterable(validEntries), 4)
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
        return DataBufferUtils.join(filePart.content()).flatMap(dataBuffer -> {
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
}