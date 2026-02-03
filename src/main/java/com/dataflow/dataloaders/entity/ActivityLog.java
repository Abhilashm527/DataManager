package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ActivityLog {
    
    private String id;
    private String activityType;
    private String entityType;
    private String entityId;
    private String entityName;
    private String action;
    private String description;
    private String userId;
    private Long createdAt;
}