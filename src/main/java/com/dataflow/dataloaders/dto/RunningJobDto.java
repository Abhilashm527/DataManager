package com.dataflow.dataloaders.dto;

import lombok.Data;
import java.util.Map;

@Data
public class RunningJobDto {
    private String id;
    private String config;
    private boolean autoRestart;
    private boolean inProgress;
    private String dataloaderId;
    private String dataloaderIdOverride;
    private String nextRun;
    private Map<String, Object> lastRun;
    private Integer lastExitCode;
    private String lastJobId;
    private Long lastReadCount;
    private Long lastSkipCount;
    private Long lastWriteCount;
    private Long lastRunParam;
}