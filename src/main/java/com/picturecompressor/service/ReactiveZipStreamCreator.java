package com.picturecompressor.service;

import com.picturecompressor.dto.ZipEntryData;
import com.picturecompressor.exception.ProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.io.*;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Slf4j
@Service
public class ReactiveZipStreamCreator {

    private static final String DELIMITER = "_";
    private static final char DOT_DELIMITER = '.';
    private static final String DEFAULT_FILENAME = "file";
    private static final int CHUNK_SIZE = 8192;

    /**
     * Streaming reactive packaging ZIP: at the output of flux <byte []>
     */
    public Flux<byte[]> createZipStream(Flux<ZipEntryData> entries) {
        return Flux.create(emitter -> Schedulers.boundedElastic().schedule(() -> {
            Set<String> usedFilenames = ConcurrentHashMap.newKeySet();
            try (PipedOutputStream out = new PipedOutputStream();
                 PipedInputStream in = new PipedInputStream(out, CHUNK_SIZE * 2);
                 ZipOutputStream zipOut = new ZipOutputStream(out)) {
                Thread readerThread = new Thread(() -> read(emitter, in));
                readerThread.start();
                write(entries, emitter, usedFilenames, zipOut, out);
            } catch (Exception e) {
                emitter.error(new ProcessingException("Error creating ZIP archive", e));
            }
        }), FluxSink.OverflowStrategy.BUFFER);
    }

    /**
     * We write Zip Entries sequentially as we get.
     *
     * @param entries - flux of ZipEntryData
     * @param emitter - flux sink
     * @param usedFilenames - set of used filenames
     * @param zipOut - ZipOutputStream
     * @param out - PipedOutputStream
     */
    private void write(Flux<ZipEntryData> entries,
                       FluxSink<byte[]> emitter,
                       Set<String> usedFilenames,
                       ZipOutputStream zipOut,
                       PipedOutputStream out) {
        entries.doOnNext(entry -> {
                    byte[] data = entry.data();
                    if (data == null || data.length == 0) {
                        log.warn("Skipping empty or null data for entry: {}", entry.filename());
                        return;
                    }
                    String entryName = generateUniqueFilename(entry.filename(), usedFilenames);
                    try {
                        ZipEntry zipEntry = new ZipEntry(entryName);
                        zipOut.putNextEntry(zipEntry);
                        zipOut.write(data);
                        zipOut.closeEntry();
                        log.debug("Added file to ZIP: {}, size: {} bytes", entryName, data.length);
                    } catch (IOException e) {
                        log.error("Failed to write ZIP entry: {}", entryName, e);
                        emitter.error(new ProcessingException("Failed to write ZIP entry: " + entryName, e));
                    }
                })
                .doOnError(emitter::error)
                .doOnComplete(() -> {
                    try {
                        zipOut.finish();
                        zipOut.flush();
                        out.close();
                    } catch (IOException e) {
                        log.error("Error finishing ZIP", e);
                        emitter.error(new ProcessingException("Error finishing ZIP", e));
                    }
                })
                .subscribe();
    }


    /**
     * Generation of a unique name for Zip Entry
     */
    private String generateUniqueFilename(String filename, Set<String> usedFilenames) {
        String name = (filename != null && !filename.isBlank()) ? filename : DEFAULT_FILENAME;
        int extIndex = name.lastIndexOf(DOT_DELIMITER);
        String base = extIndex == -1 ? name : name.substring(0, extIndex);
        String ext = extIndex == -1 ? "" : name.substring(extIndex);
        int count = 1;
        String candidate = name;
        while (!usedFilenames.add(candidate)) {
            candidate = base + DELIMITER + (count++) + ext;
        }
        return candidate;
    }

    /**
     * Launching a separate stream for reading data and emit in Flux
     *
     * @param emitter FluxSink
     * @param in PipedInputStream
     */
    private void read(FluxSink<byte[]> emitter, PipedInputStream in) {
        try {
            byte[] buffer = new byte[CHUNK_SIZE];
            int len;
            while ((len = in.read(buffer)) != -1) {
                emitter.next(copyOf(buffer, len));
            }
            emitter.complete();
        } catch (IOException e) {
            emitter.error(e);
        }
    }

    /**
     * A copy of the array to the desired length (in flux <byte []> you only need virtually a few data)
     */
    private byte[] copyOf(byte[] arr, int len) {
        byte[] res = new byte[len];
        System.arraycopy(arr, 0, res, 0, len);
        return res;
    }
}

