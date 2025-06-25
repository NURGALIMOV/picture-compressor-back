package com.picturecompressor.config;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.text.DecimalFormat;

@Component
public class HealthConfig implements HealthIndicator {

    private static final DecimalFormat FORMAT = new DecimalFormat("#.##");
    public static final double MB_DIVISOR = (double) 1024 * 1024;
    private static final int MEMORY_THRESHOLD_PERCENT = 85;

    @Override
    public Health health() {
        try {
            MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
            MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
            MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();
            
            long maxMemory = heapMemoryUsage.getMax();
            long usedMemory = heapMemoryUsage.getUsed();
            double memoryUsedPercent = (double) usedMemory / maxMemory * 100;
            
            Health.Builder builder = memoryUsedPercent < MEMORY_THRESHOLD_PERCENT ? Health.up() : Health.down();
            
            return builder
                    .withDetail("service", "GIF Compression Service")
                    .withDetail("status", memoryUsedPercent < MEMORY_THRESHOLD_PERCENT ? "Available" : "Memory pressure")
                    .withDetail("heap.used", FORMAT.format(usedMemory / MB_DIVISOR) + " MB")
                    .withDetail("heap.max", FORMAT.format(maxMemory / MB_DIVISOR) + " MB")
                    .withDetail("heap.used_percent", FORMAT.format(memoryUsedPercent) + "%")
                    .withDetail("nonHeap.used", FORMAT.format(nonHeapMemoryUsage.getUsed() / MB_DIVISOR) + " MB")
                    .withDetail("jvm.threads.count", ManagementFactory.getThreadMXBean().getThreadCount())
                    .build();
        } catch (Exception e) {
            return Health.down()
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }
} 