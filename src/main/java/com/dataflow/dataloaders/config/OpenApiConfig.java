package com.dataflow.dataloaders.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.Contact;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public GroupedOpenApi publicApi() {
        return GroupedOpenApi.builder()
                .group("dataloaders-public")
                .pathsToMatch("/api/**")
                .build();
    }

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Dataloaders API")
                        .version("1.0.0")
                        .description("API documentation for Dataloaders application")
                        .contact(new Contact()
                                .name("Dataloaders Team")
                                .email("support@dataloaders.com")));
    }
}