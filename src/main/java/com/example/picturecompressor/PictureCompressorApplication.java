package com.example.picturecompressor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.reactive.config.EnableWebFlux;

/**
 * Main application class for GIF Image Compression Service
 */
@SpringBootApplication
@EnableWebFlux
public class PictureCompressorApplication {

    public static void main(String[] args) {
        SpringApplication.run(PictureCompressorApplication.class, args);
    }
} 