package com.dataflow.dataloaders.entity;

import lombok.Data;

@Data
public class AuditMetadata {

    private Long createdAt;
    private String createdBy;
    private Long updatedAt;
    private String updatedBy;
}
