package com.example.picturecompressor.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.Contact;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for OpenAPI documentation
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Compression API")
                        .version("1.0")
                        .description("API for compressing images with adjustable compression level")
                        .contact(new Contact()
                                .name("API Support")
                                .email("support@example.com")));
    }
} 