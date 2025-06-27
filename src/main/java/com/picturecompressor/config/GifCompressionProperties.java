package com.picturecompressor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "gif-compression")
public class GifCompressionProperties {
    private long maxFileSize;
    private double maxMemoryPercent = 80;
    private int estimatedMemoryFactor = 4;
    private int maxBatchSize = 5;
    private boolean memoryCheckEnabled = true;
}