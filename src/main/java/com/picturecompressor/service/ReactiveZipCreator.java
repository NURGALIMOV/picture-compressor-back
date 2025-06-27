package com.picturecompressor.service;

import com.picturecompressor.dto.ZipEntryData;
import com.picturecompressor.exception.ProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Non-blocking ZIP archive creator using reactive patterns
 */
@Slf4j
@Service
public class ReactiveZipCreator {

    private static final String DELIMITER = "_";
    private static final char DOT_DELIMITER = '.';
    private static final int NOT_FOUND_INDEX = -1;
    private static final String DEFAULT_FILENAME = "file";


    /**
     * Create a ZIP archive reactively with parallel processing
     *
     * @param entries Flux of ZIP entry data
     * @param parallelism Level of parallelism for processing entries
     * @return Mono containing the ZIP archive as a byte array
     */
    public Mono<byte[]> createZipParallel(Flux<ZipEntryData> entries, int parallelism) {
        log.debug("Using parallel ZIP processing with concurrency: {}", parallelism);
        return entries.parallel(parallelism)
                .runOn(Schedulers.boundedElastic())
                .sequential()
                .collectList()
                .flatMap(
                        entryList -> entryList.isEmpty() ?
                                Mono.error(new ProcessingException("No valid entries provided for ZIP archive")) :
                                Mono.fromCallable(() -> createZipBytes(entryList)).subscribeOn(Schedulers.boundedElastic())
                );
    }

    /**
     * Forms an array of byte ZIP archive.
     */
    private byte[] createZipBytes(List<ZipEntryData> entryList) {
        Set<String> usedFilenames = ConcurrentHashMap.newKeySet();
        boolean hasValidEntries = false;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(GifCompressionService.ONE_MEGA_BYTE);
             ZipOutputStream zipOut = new ZipOutputStream(baos)) {
            for (ZipEntryData entry : entryList) {
                byte[] data = entry.data();
                if (Objects.isNull(data) || data.length == 0) {
                    log.warn("Skipping empty or null data for entry: {}", entry.filename());
                    continue;
                }
                hasValidEntries = true;
                String entryName = generateUniqueFilename(entry.filename(), usedFilenames);
                ZipEntry zipEntry = new ZipEntry(entryName);
                zipOut.putNextEntry(zipEntry);
                zipOut.write(data);
                zipOut.closeEntry();
                log.debug("Added file to ZIP: {}, size: {} bytes", entryName, data.length);
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
        String name = StringUtils.isNoneBlank(filename) ? filename : DEFAULT_FILENAME;
        int extIndex = name.lastIndexOf(DOT_DELIMITER);
        String base = (extIndex == NOT_FOUND_INDEX) ? name : name.substring(0, extIndex);
        String ext = (extIndex == NOT_FOUND_INDEX) ? StringUtils.EMPTY : name.substring(extIndex);
        int count = 1;
        String candidate = name;
        while (!usedFilenames.add(candidate)) {
            candidate = base + DELIMITER + (count++) + ext;
        }
        return candidate;
    }
} 