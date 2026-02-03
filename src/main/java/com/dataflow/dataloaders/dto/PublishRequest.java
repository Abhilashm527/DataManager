package com.dataflow.dataloaders.dto;

import lombok.Data;

@Data
public class PublishRequest {
    private String jobId;
    private String version;
}