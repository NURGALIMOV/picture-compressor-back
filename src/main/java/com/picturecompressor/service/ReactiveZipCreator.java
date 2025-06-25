package com.picturecompressor.service;

import com.picturecompressor.exception.ProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Non-blocking ZIP archive creator using reactive patterns
 */
@Slf4j
@Component
public class ReactiveZipCreator {

    private static final String DELIMITER = "_";
    private static final char DOT_DELIMITER = '.';
    private static final int NOT_FOUND_INDEX = -1;
    private static final int START_COUNT = 1;
    
    @Value("${gif-compression.constrained-mode:false}")
    private boolean constrainedMode;

    /**
     * Record representing a ZIP entry
     */
    public record ZipEntryData(String filename, byte[] data) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ZipEntryData that = (ZipEntryData) o;
            return Objects.deepEquals(data, that.data) && Objects.equals(filename, that.filename);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filename, Arrays.hashCode(data));
        }

        @Override
        public String toString() {
            return "ZipEntryData{" +
                    "filename='" + filename + '\'' +
                    ", data=" + Arrays.toString(data) +
                    '}';
        }
    }

    /**
     * Create a ZIP archive reactively with parallel processing
     *
     * @param entries Flux of ZIP entry data
     * @param parallelism Level of parallelism for processing entries
     * @return Mono containing the ZIP archive as a byte array
     */
    public Mono<byte[]> createZipParallel(Flux<ZipEntryData> entries, int parallelism) {
        // Use appropriate schedulers and processing strategy based on environment
        if (constrainedMode || parallelism <= 1) {
            log.debug("Using sequential ZIP processing for constrained mode");
            return entries
                .collectList()
                .flatMap(
                        entryList -> entryList.isEmpty() ?
                                Mono.error(new ProcessingException("No valid entries provided for ZIP archive")) :
                                Mono.fromCallable(() -> getBytes(entryList)).subscribeOn(Schedulers.boundedElastic())
                );
        } else {
            log.debug("Using parallel ZIP processing with concurrency: {}", parallelism);
            return entries
                .parallel(parallelism)
                .runOn(Schedulers.boundedElastic())
                .map(entry -> entry)
                .sequential()
                .collectList()
                .flatMap(
                        entryList -> entryList.isEmpty() ?
                                Mono.error(new ProcessingException("No valid entries provided for ZIP archive")) :
                                Mono.fromCallable(() -> getBytes(entryList)).subscribeOn(Schedulers.boundedElastic())
                );
        }
    }

    private byte[] getBytes(List<ZipEntryData> entryList) {
        Set<String> usedFilenames = ConcurrentHashMap.newKeySet();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(constrainedMode ? 512 * 1024 : 1024 * 1024);
             ZipOutputStream zipOut = new ZipOutputStream(baos)) {
             
            // Lower compression level in constrained mode to reduce CPU usage
            if (constrainedMode) {
                zipOut.setLevel(1); // Fastest compression
            }
             
            boolean hasValidEntries = false;
            for (ZipEntryData entry : entryList) {
                if (entry.data != null && entry.data.length > 0) {
                    hasValidEntries = true;
                    String entryName = generateUniqueFilename(entry.filename, usedFilenames);
                    ZipEntry zipEntry = new ZipEntry(entryName);
                    zipOut.putNextEntry(zipEntry);
                    zipOut.write(entry.data);
                    zipOut.closeEntry();
                    log.debug("Added file to ZIP: {}, size: {} bytes", entryName, entry.data.length);
                    
                    // Force more frequent GC in constrained mode
                    if (constrainedMode && entry.data.length > 1024 * 1024) {
                        System.gc();
                    }
                }
            }
            if (!hasValidEntries) {
                throw new ProcessingException("No valid data to include in ZIP archive");
            }
            zipOut.finish();
            zipOut.flush();
            byte[] result = baos.toByteArray();
            if (result.length == 0) {
                throw new ProcessingException("Generated ZIP file is empty");
            }
            log.info("Successfully created ZIP archive, size: {} bytes", result.length);
            return result;
        } catch (Exception e) {
            log.error("Error creating ZIP archive", e);
            throw new ProcessingException("Error creating ZIP archive", e);
        }
    }

    /**
     * Generate a unique filename for ZIP entries
     *
     * @param filename Base filename
     * @param usedFilenames Set of already used filenames
     * @return Unique filename
     */
    private String generateUniqueFilename(String filename, Set<String> usedFilenames) {
        String entryName = filename;
        AtomicInteger counter = new AtomicInteger(START_COUNT);
        while (usedFilenames.contains(entryName)) {
            int extIndex = filename.lastIndexOf(DOT_DELIMITER);
            int count = counter.getAndIncrement();
            if (extIndex == NOT_FOUND_INDEX) {
                entryName = filename + DELIMITER + count;
            } else {
                entryName = filename.substring(0, extIndex) + DELIMITER + count + filename.substring(extIndex);
            }
        }
        usedFilenames.add(entryName);
        return entryName;
    }
} 