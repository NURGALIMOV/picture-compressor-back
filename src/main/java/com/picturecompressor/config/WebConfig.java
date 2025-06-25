package com.picturecompressor.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.config.ResourceHandlerRegistry;
import org.springframework.web.reactive.config.WebFluxConfigurer;

/**
 * Additional global configuration for Webflux in a non-blocking application
 * Note: CORS configuration is handled by CorsConfig class
 */
@Configuration
@EnableWebFlux
public class WebConfig implements WebFluxConfigurer {

    /**
     * Configure resources for the WebFlux application
     */
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/swagger-ui/**")
                .addResourceLocations("classpath:/META-INF/resources/webjars/swagger-ui/");
    }
} 