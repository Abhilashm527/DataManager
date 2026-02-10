package com.dataflow.dataloaders.entity;

import lombok.Data;

@Data
public class AuditMetaData {
    private Long createdAt;
    private String createdBy;
    private Long updatedAt;
    private String updatedBy;
    private Long deletedAt;
}
