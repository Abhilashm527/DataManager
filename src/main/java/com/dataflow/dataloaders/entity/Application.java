package com.dataflow.dataloaders.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Application {
    private String id;
    private String name;
    private String environment; // DEVELOPMENT, TESTING, PRODUCTION
    private String description;
    private String createdBy;
    private Long createdAt;
    private String updatedBy;
    private Long updatedAt;
    private Long deletedAt;
}