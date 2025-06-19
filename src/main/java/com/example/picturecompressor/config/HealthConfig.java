package com.example.picturecompressor.config;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class HealthConfig implements HealthIndicator {

    @Override
    public Health health() {
        try {
            return Health.up()
                    .withDetail("service", "GIF Compression Service")
                    .withDetail("status", "Available")
                    .build();
        } catch (Exception e) {
            return Health.down()
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }
} 