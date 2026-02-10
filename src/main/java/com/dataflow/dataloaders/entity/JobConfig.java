package com.dataflow.dataloaders.entity;

import com.dataflow.dataloaders.dto.SourceConfig;
import com.dataflow.dataloaders.dto.TargetConfig;
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
public class JobConfig extends AuditMetaData {

    private String jobId;
    private String parentJobId;
    private Boolean drafted;
    private String jobName;
    private String jobDescription;
    private String impacts;
    private String jobSeverity;
    private Integer chunkSize;
    

    
    private String mappingId;

    private SourceConfig sourceConfig;
    private TargetConfig targetConfig;
    
    private Boolean scheduled;
    private String schedule;
    
    private Boolean published;
    private String publishedVersion;

    private String deployedVersion;
    private Boolean deployed;


    // Additional fields for consistency with Resource pattern
    private String status;
    private Boolean isActive;
    private String itemId;
}
