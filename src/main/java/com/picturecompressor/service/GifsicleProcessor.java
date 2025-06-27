package com.picturecompressor.service;

import com.picturecompressor.exception.ProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Non-closing processor for compressing GIF files using
 * external GIFSicle utility, which consumes much less memory,
 * than native java realeization
 */
@Slf4j
@Service
public class GifsicleProcessor {

    @Value("${gifsicle.path:gifsicle}")
    private String gifsicleExecutable;
    @Value("${gifsicle.timeout-seconds:10}")
    private int timeoutSeconds;
    @Value("${gifsicle.optimization-level:3}")
    private int optimizationLevel;
    @Value("${gifsicle.use-lossy:false}")
    private boolean useLossy;
    private final AtomicBoolean gifsicleAvailableCached = new AtomicBoolean(false);
    private volatile boolean gifsicleAvailabilityChecked = false;
    
    /**
     * Clutch GIF file using gifsicle
     *
     * @param gifBytes The starting bytes of the GIF file
     * @param compressionLevel compression level (0-1), where 0 - maximum compression, 1 - without compression
     * @return Mono with bytes of a compressed GIF file
     */
    public Mono<byte[]> compressGif(byte[] gifBytes, float compressionLevel) {
        if (compressionLevel >= 1) {
            log.info("Compression level is 0, returning original GIF. Size: {} bytes", gifBytes.length);
            return Mono.just(gifBytes);
        }
        return Mono.fromCallable(() -> {
            Path tempInputFile = null;
            Path tempOutputFile = null;
            try {
                tempInputFile = Files.createTempFile("input_", ".gif");
                tempOutputFile = Files.createTempFile("output_", ".gif");
                Files.write(tempInputFile, gifBytes);
                List<String> command = buildGifsicleCommand(tempInputFile, tempOutputFile, compressionLevel);
                log.debug("Executing gifsicle command: {}", String.join(" ", command));
                ProcessBuilder processBuilder = new ProcessBuilder(command);
                long start = System.currentTimeMillis();
                Process process = processBuilder.start();
                StringBuilder errorOutput = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        errorOutput.append(line).append("\n");
                    }
                }
                boolean completed = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
                if (!completed) {
                    process.destroyForcibly();
                    throw new ProcessingException("Gifsicle process timed out after %s seconds".formatted(timeoutSeconds));
                }
                int exitCode = process.exitValue();
                if (exitCode != 0) {
                    log.error("Gifsicle failed with exit code {}: {}", exitCode, errorOutput);
                    throw new ProcessingException("Gifsicle failed with exit code %s : %s".formatted(exitCode, errorOutput));
                }
                byte[] result = Files.readAllBytes(tempOutputFile);
                long duration = System.currentTimeMillis() - start;
                log.info(
                        "GIF successfully compressed. Orig: {} bytes, Compressed: {} bytes, Ratio: {}%, CompressionLevel: {}, Time: {} ms",
                        gifBytes.length,
                        result.length,
                        gifBytes.length > 0 ? String.format("%.2f", (float) result.length / gifBytes.length * 100) : "N/A",
                        compressionLevel,
                        duration
                );
                return result;
            } finally {
                deleteTempFileIfExists(tempInputFile);
                deleteTempFileIfExists(tempOutputFile);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
    
    /**
     * Builds a command for Gifsicle depending on the compression level
     */
    private List<String> buildGifsicleCommand(Path inputFile, Path outputFile, float compressionLevel) {
        List<String> command = new ArrayList<>();
        command.add(gifsicleExecutable);
        int optLevel = Math.max(1, (int) Math.ceil(optimizationLevel * compressionLevel));
        command.add("--optimize=" + optLevel);
        if (useLossy) {
            int actualLossyLevel = (int) (100 - compressionLevel * 100);
            command.add("--lossy=" + actualLossyLevel);
        }
        if (compressionLevel > 0.7) {
            command.add("--colors=128");
        } else if (compressionLevel > 0.4) {
            command.add("--colors=192");
        }
        command.add("-i");
        command.add(inputFile.toString());
        command.add("-o");
        command.add(outputFile.toString());
        return command;
    }

    /**
     * Checks whether Gifsicle is available in the system (cached for performance).
     *
     * @return true, if Gifsicle is available
     */
    public boolean isGifsicleAvailable() {
        if (!gifsicleAvailabilityChecked) {
            gifsicleAvailableCached.set(checkGifsicleAvailability());
            gifsicleAvailabilityChecked = true;
        }
        return gifsicleAvailableCached.get();
    }

    // Helper method for actual gifsicle check
    private boolean checkGifsicleAvailability() {
        try {
            Process process = new ProcessBuilder(gifsicleExecutable, "--version").start();
            boolean completed = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            if (!completed) {
                process.destroyForcibly();
                log.warn("Gifsicle check timed out");
                return false;
            }
            int exitCode = process.exitValue();
            if (exitCode == 0) {
                log.info("Gifsicle is available: {}.", gifsicleExecutable);
            } else {
                log.warn("Gifsicle is NOT available. Exit code: {}", exitCode);
            }
            return exitCode == 0;
        } catch (InterruptedException e) {
            log.warn("Interrupted during gifsicle check!", e);
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            log.warn("Gifsicle is not available: {}", e.getMessage());
            return false;
        }
    }

    // Helper for safe temp file removal
    private void deleteTempFileIfExists(Path path) {
        if (Objects.nonNull(path)) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                log.warn("Failed to delete temporary file: {}", path, e);
            }
        }
    }
} 