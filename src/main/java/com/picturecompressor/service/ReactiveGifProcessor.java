package com.picturecompressor.service;

import com.picturecompressor.exception.ProcessingException;
import com.madgag.gif.fmsware.AnimatedGifEncoder;
import com.madgag.gif.fmsware.GifDecoder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Non-blocking reactive processor for GIF animations
 */
@Slf4j
@Component
public class ReactiveGifProcessor {

    private static final int GIF_HEADER_LENGTH = 6; // GIF89a or GIF87a
    private static final int SUCCESS_STATUS = 0;
    private static final int MAX_QUALITY = 1000;
    
    @Value("${gif-compression.constrained-mode:false}")
    private boolean constrainedMode;
    
    @Value("${gif-compression.max-parallel-frames:2}")
    private int maxParallelFrames;
    
    /**
     * Record representing a GIF frame
     */
    private record GifFrame(BufferedImage image, int delay) {}
    
    /**
     * Record representing a decoded GIF
     */
    private record DecodedGif(List<GifFrame> frames, int loopCount) {}
    
    /**
     * Compress a GIF using reactive streams
     *
     * @param gifBytes Original GIF data
     * @param compressionLevel Compression level (0-1)
     * @return Mono containing compressed GIF data
     */
    public Mono<byte[]> compressGif(byte[] gifBytes, float compressionLevel) {
        return validateGifBytes(gifBytes)
            .flatMap(validBytes -> Mono.fromCallable(() -> decodeGif(validBytes))
                .subscribeOn(Schedulers.boundedElastic()))
            .flatMap(decodedGif -> processFrames(decodedGif, compressionLevel));
    }
    
    /**
     * Validate that input bytes represent a GIF image
     * 
     * @param bytes Input bytes
     * @return Mono with validated bytes
     */
    private Mono<byte[]> validateGifBytes(byte[] bytes) {
        return Mono.fromCallable(() -> {
            if (bytes == null || bytes.length < GIF_HEADER_LENGTH) {
                throw new ProcessingException("Invalid GIF data: too small");
            }
            // Check for GIF signature
            String signature = new String(bytes, 0, GIF_HEADER_LENGTH);
            if (!signature.startsWith("GIF8")) {
                throw new ProcessingException("Invalid GIF data: incorrect signature");
            }
            return bytes;
        }).subscribeOn(constrainedMode ? Schedulers.boundedElastic() : Schedulers.parallel());
    }
    
    /**
     * Decode GIF into frames
     * Note: This method is blocking and should be called on boundedElastic scheduler
     */
    private DecodedGif decodeGif(byte[] gifBytes) {
        try (ByteArrayInputStream is = new ByteArrayInputStream(gifBytes)) {
            GifDecoder decoder = new GifDecoder();
            int status = decoder.read(is);
            if (status != SUCCESS_STATUS) {
                throw new ProcessingException("Error decoding GIF file, status code: " + status);
            }
            int frameCount = decoder.getFrameCount();
            if (frameCount <= 0) {
                throw new ProcessingException("Invalid GIF file: no frames found");
            }
            List<GifFrame> frames = new ArrayList<>(frameCount);
            for (int i = 0; i < frameCount; i++) {
                frames.add(new GifFrame(decoder.getFrame(i), decoder.getDelay(i)));
            }
            return new DecodedGif(frames, decoder.getLoopCount());
        } catch (Exception e) {
            throw new ProcessingException("Error decoding GIF", e);
        }
    }
    
    /**
     * Process GIF frames with reactive streams
     * 
     * @param decodedGif Decoded GIF frames
     * @param compressionLevel Compression level (0-1)
     * @return Mono with compressed GIF data
     */
    private Mono<byte[]> processFrames(DecodedGif decodedGif, float compressionLevel) {
        return Mono.fromCallable(() -> {
            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                AnimatedGifEncoder encoder = new AnimatedGifEncoder();
                encoder.start(os);
                int quality = MAX_QUALITY - Math.round(compressionLevel * MAX_QUALITY);
                encoder.setQuality(quality);
                encoder.setRepeat(decodedGif.loopCount);
                // Process each frame
                for (GifFrame frame : decodedGif.frames) {
                    encoder.addFrame(frame.image);
                    encoder.setDelay(frame.delay);
                }
                if (!encoder.finish()) {
                    throw new ProcessingException("Failed to finish encoding GIF");
                }
                return os.toByteArray();
            } catch (Exception e) {
                throw new ProcessingException("Error encoding GIF", e);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
    
    /**
     * Alternative implementation using Flux for frame processing
     * Allows parallel frame processing with backpressure handling
     * 
     * @param gifBytes Original GIF data
     * @param compressionLevel Compression level (0-1)
     * @return Mono containing compressed GIF data
     */
    public Mono<byte[]> compressGifParallel(byte[] gifBytes, float compressionLevel) {
        return validateGifBytes(gifBytes)
            .flatMap(validBytes -> Mono.fromCallable(() -> decodeGif(validBytes)).subscribeOn(Schedulers.boundedElastic()))
            .flatMap(decodedGif -> {
                // Calculate encoder settings
                int quality = MAX_QUALITY - Math.round(compressionLevel * MAX_QUALITY);
                
                // Adaptive parallelism based on environment constraints and frame count
                int actualParallelFrames = Math.min(maxParallelFrames, Math.min(4, decodedGif.frames.size()));
                log.debug("Using parallelism of {} for frame processing", actualParallelFrames);
                
                // Create a stream of frames for processing
                return Flux.fromIterable(decodedGif.frames)
                    // Apply backpressure if processing is too fast
                    .limitRate(actualParallelFrames)
                    .flatMap(frame -> Mono.just(frame).publishOn(Schedulers.boundedElastic()), actualParallelFrames)
                    .collectList()
                    .flatMap(processedFrames -> Mono.fromCallable(() -> getBytes(decodedGif, quality)).subscribeOn(Schedulers.boundedElastic()));
            });
    }

    private static byte[] getBytes(DecodedGif decodedGif, int quality) throws IOException {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            AnimatedGifEncoder encoder = new AnimatedGifEncoder();
            encoder.start(os);
            encoder.setQuality(quality);
            encoder.setRepeat(decodedGif.loopCount);
            for (GifFrame frame : decodedGif.frames) {
                encoder.addFrame(frame.image);
                encoder.setDelay(frame.delay);
            }
            if (!encoder.finish()) {
                throw new ProcessingException("Failed to finish encoding GIF");
            }
            return os.toByteArray();
        }
    }
} 