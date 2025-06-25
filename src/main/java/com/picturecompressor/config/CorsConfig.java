package com.picturecompressor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsWebFilter;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.config.WebFluxConfigurer;

import java.util.Arrays;
import java.util.function.Consumer;

/**
 * Configuration class for CORS settings in non-blocking WebFlux application
 */
@Configuration
@EnableWebFlux
public class CorsConfig implements WebFluxConfigurer {

    @Value("${spring.cors.allowed-origins}")
    private String allowedOrigins;
    
    @Value("${spring.cors.allowed-methods}")
    private String allowedMethods;
    
    @Value("${spring.cors.allowed-headers}")
    private String allowedHeaders;
    
    @Value("${spring.cors.exposed-headers}")
    private String exposedHeaders;
    
    @Value("${spring.cors.allow-credentials}")
    private boolean allowCredentials;
    
    @Value("${spring.cors.max-age}")
    private long maxAge;

    @Bean
    public CorsWebFilter corsWebFilter() {
        CorsConfiguration corsConfig = new CorsConfiguration();
        handleAllowed(allowedOrigins, s -> corsConfig.addAllowedOriginPattern(s.trim()), s -> corsConfig.addAllowedOrigin(s.trim()));
        handleAllowed(allowedMethods, s -> corsConfig.addAllowedMethod(s.trim()), s -> corsConfig.addAllowedMethod(s.trim()));
        handleAllowed(allowedHeaders, s -> corsConfig.addAllowedHeader(s.trim()), s -> corsConfig.addAllowedHeader(s.trim()));
        handleAllowed(exposedHeaders, s -> corsConfig.addExposedHeader(s.trim()), s -> corsConfig.addExposedHeader(s.trim()));
        corsConfig.setAllowCredentials(allowCredentials);
        corsConfig.setMaxAge(maxAge);
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", corsConfig);
        return new CorsWebFilter(source);
    }

    private void handleAllowed(String allowed, Consumer<String> pattern, Consumer<String> consumer) {
        if ("*".equals(allowed)) {
            pattern.accept(allowed);
            return;
        }
        Arrays.stream(allowed.split(",")).forEach(consumer);
    }
} 