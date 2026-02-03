package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobConfigReference extends AuditMetadata {
    
    private String id;
    private String itemId;
    private String jobId;
}