package com.picturecompressor.config;

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
    
    /**
     * Configure custom connection provider for Reactor Netty
     */
    @Bean
    public ConnectionProvider connectionProvider() {
        return ConnectionProvider.builder("default")
                .maxConnections(500)
                .maxIdleTime(Duration.ofSeconds(60))
                .maxLifeTime(Duration.ofMinutes(5))
                .pendingAcquireTimeout(Duration.ofSeconds(60))
                .build();
    }
    
    /**
     * Configure worker threads for Reactor Netty
     */
    @Bean
    public LoopResources loopResources() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int workerCount = Math.max(2, availableProcessors);
        int selectCount = Math.max(1, availableProcessors / 2);
        return LoopResources.create("reactor-http", selectCount, workerCount, true);
    }
} 