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
public class ConnectionRequest {
    private Long userId;
    private Long providerId;
    private String connectionName;
    private JsonNode config;
    private JsonNode secrets;
    private Boolean useSsl;
    private Integer connectionTimeout;
}
