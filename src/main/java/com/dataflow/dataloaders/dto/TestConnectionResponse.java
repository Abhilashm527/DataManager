package com.dataflow.dataloaders.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TestConnectionResponse {
    private Boolean success;
    private String status;
    private String message;
    private Integer responseTimeMs;
    private String errorCode;
    private String errorMessage;
    private JsonNode serverInfo;
    private List<String> troubleshooting;
}
