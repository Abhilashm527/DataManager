package com.dataflow.dataloaders.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataflowCanvasDto {
    private WorkflowInfoDto workflow;
    private JsonNode canvasState;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkflowInfoDto {
        private String dataflowId;
        private String name;
        private String updatedAt;
    }
}
