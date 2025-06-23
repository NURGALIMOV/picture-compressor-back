package com.example.picturecompressor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

import java.time.Duration;

/**
 * Programmatic configuration for Reactor Netty
 * These settings cannot be configured via properties files
 */
@Configuration
public class ReactorNettyConfig {

    @Value("${gif-compression.constrained-mode:false}")
    private boolean constrainedMode;
    
    /**
     * Configure custom connection provider for Reactor Netty
     */
    @Bean
    public ConnectionProvider connectionProvider() {
        if (constrainedMode) {
            // Limited resources for constrained environments
            return ConnectionProvider.builder("constrained")
                    .maxConnections(256)
                    .maxIdleTime(Duration.ofSeconds(30))
                    .maxLifeTime(Duration.ofMinutes(1))
                    .pendingAcquireTimeout(Duration.ofSeconds(60))
                    .build();
        } else {
            // Default settings
            return ConnectionProvider.builder("default")
                    .maxConnections(500)
                    .maxIdleTime(Duration.ofSeconds(60))
                    .maxLifeTime(Duration.ofMinutes(5))
                    .pendingAcquireTimeout(Duration.ofSeconds(60))
                    .build();
        }
    }
    
    /**
     * Configure worker threads for Reactor Netty
     */
    @Bean
    public LoopResources loopResources() {
        int workerCount = constrainedMode ? 2 : Runtime.getRuntime().availableProcessors();
        int selectCount = constrainedMode ? 1 : workerCount / 2;
        
        return LoopResources.create("reactor-http", selectCount, workerCount, true);
    }
} 