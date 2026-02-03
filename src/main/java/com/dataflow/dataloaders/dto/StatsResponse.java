package com.dataflow.dataloaders.dto;

import lombok.Data;

@Data
public class StatsResponse {
    private int activeDeployments;
    private int inactiveDeployments;
    private int scheduledJobs;
    private int manualJobs;
    private double successRate;
    private int successCount;
    private int failureCount;
    private int stoppedCount;
}
