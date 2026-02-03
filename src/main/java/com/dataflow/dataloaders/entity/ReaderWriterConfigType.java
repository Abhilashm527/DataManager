package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReaderWriterConfigType {

    private String id;
    private String typeName;              // "MySQL", "SFTP", "MongoDB", etc.
    private String category;              // "READER" or "WRITER"
    private String description;
    
    // Base configuration that's always present
    private Map<String, Object> baseConfig;
    
    // Dynamic fields that user can configure
    private List<ConfigField> configFields;
    
    private Boolean isActive;
    
    // Audit fields
    private String createdBy;
    private Long createdAt;
    private String updatedBy;
    private Long updatedAt;
}