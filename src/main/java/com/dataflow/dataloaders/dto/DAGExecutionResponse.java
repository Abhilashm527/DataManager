package com.dataflow.dataloaders.dto;

import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DAGExecutionResponse {
    private String dagName;
    private String jobStatus;
    private Long jobExecutionId;
    private String createTime;
    private String startTime;
    private String dagId;
    private String status;
}
