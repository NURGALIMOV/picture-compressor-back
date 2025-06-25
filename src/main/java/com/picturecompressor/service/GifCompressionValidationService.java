package com.picturecompressor.service;

import com.picturecompressor.exception.InvalidCompressionLevelException;
import com.picturecompressor.exception.InvalidFileTypeException;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Service for validation using fully reactive patterns
 */
@Service
public class GifCompressionValidationService {

    private GifCompressionValidationService() {
        // Private constructor to prevent instantiation
    }

    public static final String SUPPORT_FORMAT = ".gif";

    /**
     * Validates that the file is a GIF in a reactive way
     *
     * @param filePart The file to validate
     * @return A Mono that completes successfully if the file is valid, or errors with InvalidFileTypeException
     */
    public static Mono<Void> validateFileTypeReactive(FilePart filePart) {
        return Mono.defer(() -> {
            String filename = filePart.filename().toLowerCase();
            return filename.endsWith(SUPPORT_FORMAT) ? 
                Mono.empty() : 
                Mono.error(new InvalidFileTypeException("Only GIF files are allowed. Received: " + filename));
        });
    }

    /**
     * Validates that the compression level is between 0 and 1 in a reactive way
     *
     * @param compressionLevel The compression level to validate
     * @return A Mono that completes successfully if the level is valid, or errors with InvalidCompressionLevelException
     */
    public static Mono<Void> validateCompressionLevelReactive(float compressionLevel) {
        return Mono.defer(() -> 
            (compressionLevel >= 0 && compressionLevel <= 1) ? 
                Mono.empty() : 
                Mono.error(new InvalidCompressionLevelException("Compression level must be between 0 and 1"))
        );
    }

    /**
     * Performs all validations for a GIF compression request in a reactive way
     *
     * @param filePart The file to validate
     * @param compressionLevel The compression level to validate
     * @return A Mono that completes successfully if all validations pass, or errors with the appropriate exception
     */
    public static Mono<Void> validateCompressionRequest(FilePart filePart, float compressionLevel) {
        return validateFileTypeReactive(filePart)
            .then(validateCompressionLevelReactive(compressionLevel));
    }
}
