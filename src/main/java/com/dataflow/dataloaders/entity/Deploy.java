package com.dataflow.dataloaders.entity;

import lombok.Data;

@Data
public class Deploy extends AuditMetadata {
    private String deployId;
    private String parentJobId;
    private String jobId;
    private String deployName;
    private boolean manualRun = false;
    private boolean schedule;
    private String scheduleExpression;
    private Long schedulerId;
    private String schedulerName;
    private boolean active;
    private Long deletedAt;
}
