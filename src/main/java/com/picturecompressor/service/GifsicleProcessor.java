package com.picturecompressor.service;

import com.picturecompressor.exception.ProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Неблокирующий процессор для сжатия GIF-файлов с использованием 
 * внешней утилиты gifsicle, которая потребляет значительно меньше памяти,
 * чем нативная Java-реализация
 */
@Slf4j
@Component
public class GifsicleProcessor {

    @Value("${gifsicle.path:gifsicle}")
    private String gifsicleExecutable;
    
    @Value("${gifsicle.timeout-seconds:10}")
    private int timeoutSeconds;

    @Value("${gifsicle.optimization-level:3}")
    private int optimizationLevel;
    
    @Value("${gifsicle.lossy-level:30}")
    private int lossyLevel;
    
    @Value("${gifsicle.use-lossy:false}")
    private boolean useLossy;
    
    /**
     * Сжимает GIF-файл с использованием gifsicle
     *
     * @param gifBytes исходные байты GIF-файла
     * @param compressionLevel уровень сжатия (0-1), где 0 - без сжатия, 1 - максимальное сжатие
     * @return Mono с байтами сжатого GIF-файла
     */
    public Mono<byte[]> compressGif(byte[] gifBytes, float compressionLevel) {
        return Mono.fromCallable(() -> {
            Path tempInputFile = Files.createTempFile("input_", ".gif");
            Path tempOutputFile = Files.createTempFile("output_", ".gif");
            try {
                Files.write(tempInputFile, gifBytes);
                List<String> command = buildGifsicleCommand(tempInputFile, tempOutputFile, compressionLevel);
                log.debug("Executing gifsicle command: {}", String.join(" ", command));
                ProcessBuilder processBuilder = new ProcessBuilder(command);
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
                    throw new ProcessingException("Gifsicle process timed out after " + timeoutSeconds + " seconds");
                }
                int exitCode = process.exitValue();
                if (exitCode != 0) {
                    log.error("Gifsicle failed with exit code {}: {}", exitCode, errorOutput);
                    throw new ProcessingException("Gifsicle failed with exit code " + exitCode + ": " + errorOutput);
                }
                byte[] result = Files.readAllBytes(tempOutputFile);
                log.info(
                        "GIF successfully compressed with gifsicle, original size: {}, compressed size: {}, ratio: {}",
                        gifBytes.length,
                        result.length,
                        result.length > 0 ? "%.2f%%".formatted((float) result.length / gifBytes.length * 100) : "N/A"
                );
                return result;
            } finally {
                try {
                    Files.deleteIfExists(tempInputFile);
                    Files.deleteIfExists(tempOutputFile);
                } catch (IOException e) {
                    log.warn("Failed to delete temporary files", e);
                }
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
    
    /**
     * Строит команду для gifsicle в зависимости от уровня сжатия
     */
    private List<String> buildGifsicleCommand(Path inputFile, Path outputFile, float compressionLevel) {
        List<String> command = new ArrayList<>();
        command.add(gifsicleExecutable);
        int optLevel = Math.max(1, (int) Math.ceil(optimizationLevel * compressionLevel));
        command.add("--optimize=" + optLevel);
        if (useLossy) {
            int actualLossyLevel = (int) (100 - compressionLevel * 100); // масштабирование 0.5-1.0 -> 0-100%
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
     * Проверяет, доступен ли gifsicle в системе
     * 
     * @return Mono<Boolean> true, если gifsicle доступен
     */
    public Mono<Boolean> isGifsicleAvailable() {
        return Mono.fromCallable(() -> {
            try {
                Process process = new ProcessBuilder(gifsicleExecutable, "--version").start();
                boolean completed = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
                if (!completed) {
                    process.destroyForcibly();
                    log.warn("Gifsicle check timed out");
                    return false;
                }
                int exitCode = process.exitValue();
                return exitCode == 0;
            } catch (InterruptedException e) {
                log.warn("Interrupted!", e);
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception e) {
                log.warn("Gifsicle is not available: {}", e.getMessage());
                return false;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
} 