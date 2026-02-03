package com.dataflow.dataloaders.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TargetConfig {
    
    private String targetId;
    private String targetType;
    private Map<String, Object> configFields;
    private Map<String, Object> predefinedFields;
}